from typing import Mapping
from fastapi import HTTPException, status, Request

ALLOWED_ROLES = {"admin", "service", "user"}

def extract_role_from_headers(headers: Mapping[str, str]) -> str:
    """
    Extract and validate the client role from HTTP headers.

    This is the core, framework-agnostic role extraction function.
    Later, its implementation can be swapped (e.g., decode JWT) without
    changing callers.
    """

    role = headers.get("X-Client-Role")

    if role is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Client-Role header"
        )

    if role not in ALLOWED_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Role is not permitted."
        )     

    return role

def get_current_role(request: Request) -> str:
    """
    FastAPI-friendly dependency that enforces authorization.

    Usage in routes:
        from fastapi import Depends

        @router.post("/track")
        def track_event(role: str = Depends(get_current_role)):
            ...

    This keeps all role logic centralized and easily swappable.
    """
    return extract_role_from_headers(request.headers)
     
        