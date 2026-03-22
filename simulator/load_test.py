"""
Load test simulator for the telemetry ingestion API.

Sends a burst of telemetry events to POST /track to validate system stability,
throughput, and error handling under load. Uses asyncio + httpx for concurrent requests.
"""

import asyncio
import random
import time
from typing import List, Tuple

import httpx

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
TOTAL_REQUESTS = 500
API_URL = "http://localhost:8000/track"

EVENT_TYPES = ("page_view", "login", "purchase", "add_to_cart")
PAGES = ("/home", "/product", "/checkout", "/cart")


def random_event_type() -> str:
    return random.choice(EVENT_TYPES)


def random_user_id() -> str:
    return f"u{random.randint(1, 500)}"


def random_page() -> str:
    return random.choice(PAGES)


def build_payload() -> dict:
    return {
        "event_type": random_event_type(),
        "user_id": random_user_id(),
        "metadata": {"page": random_page()},
    }


async def send_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    completed: List[int],
    success_count: List[int],
    failure_count: List[int],
    error_messages: List[str],
    progress_lock: asyncio.Lock,
) -> None:
    payload = build_payload()
    async with semaphore:
        try:
            response = await client.post(
                API_URL,
                json=payload,
                headers={"X-Client-Role": "service"},
                timeout=30.0,
            )
            async with progress_lock:
                completed[0] += 1
                n = completed[0]
                if n % 100 == 0 or n == TOTAL_REQUESTS:
                    print(f"Requests completed: {n}")

            if response.status_code == 202:
                success_count[0] += 1
            else:
                failure_count[0] += 1
                if response.status_code in (401, 403):
                    error_messages.append("Unauthorized role detected")
                elif response.status_code == 422:
                    error_messages.append("Invalid telemetry payload")
                elif response.status_code >= 500:
                    error_messages.append("Server error detected")
        except httpx.ConnectError:
            async with progress_lock:
                completed[0] += 1
                n = completed[0]
                if n % 100 == 0 or n == TOTAL_REQUESTS:
                    print(f"Requests completed: {n}")
            failure_count[0] += 1
            error_messages.append("Failed to connect to API server")
        except Exception as exc:
            async with progress_lock:
                completed[0] += 1
                n = completed[0]
                if n % 100 == 0 or n == TOTAL_REQUESTS:
                    print(f"Requests completed: {n}")
            failure_count[0] += 1
            error_messages.append(str(exc))


async def run_load_test() -> Tuple[int, int, float]:
    """Send TOTAL_REQUESTS concurrently; return (success, failed, duration_sec)."""
    completed: List[int] = [0]
    success_count: List[int] = [0]
    failure_count: List[int] = [0]
    error_messages: List[str] = []

    # Semaphore set to TOTAL_REQUESTS so all run concurrently (burst)
    semaphore = asyncio.Semaphore(TOTAL_REQUESTS)
    progress_lock = asyncio.Lock()

    print(f"Sending {TOTAL_REQUESTS} telemetry events...")

    start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        tasks = [
            send_one(
                client,
                semaphore,
                completed,
                success_count,
                failure_count,
                error_messages,
                progress_lock,
            )
            for _ in range(TOTAL_REQUESTS)
        ]
        await asyncio.gather(*tasks)
    duration = time.perf_counter() - start

    # Log distinct errors (avoid spamming same message 500 times)
    for msg in set(error_messages):
        print(msg)

    return success_count[0], failure_count[0], duration


def main() -> None:
    success, failed, duration = asyncio.run(run_load_test())

    throughput = success / duration if duration > 0 else 0.0

    print()
    print("Load Test Complete")
    print()
    print(f"Total Requests: {TOTAL_REQUESTS}")
    print(f"Success: {success}")
    print(f"Failures: {failed}")
    print(f"Total Time: {duration:.2f} seconds")
    print(f"Throughput: {throughput:.1f} events/sec")
    print()
    print("Metrics (as specified):")
    print(f"Total Requests Sent: {TOTAL_REQUESTS}")
    print(f"Successful Responses: {success}")
    print(f"Failed Responses: {failed}")
    print(f"Total Execution Time: {duration:.2f} seconds")
    print(f"Throughput: {throughput:.1f} events/sec")


if __name__ == "__main__":
    main()
