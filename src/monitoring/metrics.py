import time
import logging
from typing import Dict, Any, Optional
from functools import wraps
from datetime import datetime, timedelta
from collections import defaultdict, deque
import asyncio

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collect and store application metrics"""
    
    def __init__(self):
        self.metrics = defaultdict(lambda: defaultdict(float))
        self.counters = defaultdict(int)
        self.timers = defaultdict(list)
        self.recent_requests = deque(maxlen=1000)  # Keep last 1000 requests
        self.error_counts = defaultdict(int)
        self.start_time = datetime.utcnow()
    
    def increment_counter(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric"""
        key = self._make_key(name, tags)
        self.counters[key] += value
    
    def record_timer(self, name: str, duration: float, tags: Optional[Dict[str, str]] = None):
        """Record a timing metric"""
        key = self._make_key(name, tags)
        self.timers[key].append(duration)
        
        # Keep only recent values (last 100)
        if len(self.timers[key]) > 100:
            self.timers[key] = self.timers[key][-100:]
    
    def record_gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a gauge metric"""
        key = self._make_key(name, tags)
        self.metrics[name][key] = value
    
    def record_request(self, method: str, path: str, status_code: int, duration: float):
        """Record HTTP request metrics"""
        self.recent_requests.append({
            'timestamp': datetime.utcnow(),
            'method': method,
            'path': path,
            'status_code': status_code,
            'duration': duration
        })
        
        # Increment counters
        self.increment_counter('http_requests_total', tags={
            'method': method,
            'status': str(status_code)
        })
        
        # Record timing
        self.record_timer('http_request_duration', duration, tags={
            'method': method,
            'path': path
        })
        
        # Track errors
        if status_code >= 400:
            self.increment_counter('http_errors_total', tags={
                'method': method,
                'status': str(status_code)
            })
    
    def _make_key(self, name: str, tags: Optional[Dict[str, str]] = None) -> str:
        """Create a key from name and tags"""
        if not tags:
            return name
        
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}{{tags}}"
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics"""
        
        # Calculate request rate (requests per minute)
        now = datetime.utcnow()
        recent_minute = [r for r in self.recent_requests 
                        if now - r['timestamp'] < timedelta(minutes=1)]
        request_rate = len(recent_minute)
        
        # Calculate average response time
        if self.recent_requests:
            avg_response_time = sum(r['duration'] for r in self.recent_requests) / len(self.recent_requests)
        else:
            avg_response_time = 0
        
        # Calculate error rate
        error_requests = [r for r in self.recent_requests if r['status_code'] >= 400]
        error_rate = len(error_requests) / max(len(self.recent_requests), 1) * 100
        
        # Uptime
        uptime = (now - self.start_time).total_seconds()
        
        return {
            'uptime_seconds': uptime,
            'request_rate_per_minute': request_rate,
            'avg_response_time_ms': avg_response_time * 1000,
            'error_rate_percent': error_rate,
            'total_requests': len(self.recent_requests),
            'counters': dict(self.counters),
            'timers': {k: {
                'count': len(v),
                'avg': sum(v) / len(v) if v else 0,
                'min': min(v) if v else 0,
                'max': max(v) if v else 0
            } for k, v in self.timers.items()},
            'gauges': dict(self.metrics)
        }

# Global metrics collector
metrics = MetricsCollector()

def track_time(metric_name: str, tags: Optional[Dict[str, str]] = None):
    """Decorator to track execution time of functions"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                metrics.record_timer(metric_name, duration, tags)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                metrics.record_timer(metric_name, duration, tags)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

def count_calls(metric_name: str, tags: Optional[Dict[str, str]] = None):
    """Decorator to count function calls"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            metrics.increment_counter(metric_name, tags=tags)
            return func(*args, **kwargs)
        return wrapper
    return decorator
