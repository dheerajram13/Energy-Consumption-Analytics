"""FastAPI middleware for monitoring and metrics collection."""
import time
from typing import Callable, Awaitable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from .metrics import metrics

class MonitoringMiddleware(BaseHTTPMiddleware):
    """
    Middleware to collect HTTP request metrics.
    
    This middleware tracks:
    - Request count
    - Request duration
    - Error rates
    - Response times
    """
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.time()
        
        try:
            # Process the request
            response = await call_next(request)
            duration = time.time() - start_time
            
            # Record successful request
            metrics.record_request(
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration=duration
            )
            
            # Add server timing header
            response.headers["X-Process-Time"] = str(duration)
            
            return response
            
        except Exception as e:
            # Record failed request
            duration = time.time() - start_time
            metrics.record_request(
                method=request.method,
                path=request.url.path,
                status_code=500,  # Internal Server Error
                duration=duration
            )
            raise
