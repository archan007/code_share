"""
Router Module
=============
Maps incoming requests (HTTP method + path) to handler functions.

Routes are organized by data product (Snowflake schema) for clarity.
Supports all HTTP methods (GET, POST, PUT, DELETE, PATCH).

Adding a new endpoint:
1. Create handler function in handlers/{product}.py
2. Register route below
3. Done!
"""

import re
from typing import Any, Dict

from utils.exceptions import NotFoundError
from utils.auth import validate_authorization

# Import all handler modules
from handlers import accounts, product_1, product_2


class Router:
    """
    Lightweight router mapping (method, path) -> handler function.
    
    Supports path parameters using {param_name} syntax:
        Example: /accounts/{id} matches /accounts/123
    """
    
    def __init__(self):
        self.routes = [
            
            # ===========================================================
            # GOLD_C360 - Customer 360 Data Product (3 endpoints)
            # ===========================================================
            ("GET", "/account-summary", accounts.get_account_summary),
            ("GET", "/accounts/{id}", accounts.get_account_detail),
            ("GET", "/accounts/{id}/activities", accounts.get_account_activities),
            
            # Future POST/PUT examples (uncomment when ready):
            # ("PUT", "/accounts/{id}/status", accounts.update_account_status),
            
            # ===========================================================
            # GOLD_C360 - Generic Product 1 endpoints (3 endpoints)
            # ===========================================================
            ("GET", "/endpoint-a", product_1.list_endpoint_a),
            ("GET", "/endpoint-b/{id}", product_1.get_endpoint_b_detail),
            ("GET", "/endpoint-c/summary", product_1.get_endpoint_c_summary),
            
            # ===========================================================
            # GOLD_CI - Generic Product 2 endpoints (4 endpoints)
            # ===========================================================
            ("GET", "/endpoint-d", product_2.list_endpoint_d),
            ("GET", "/endpoint-e/{id}", product_2.get_endpoint_e_detail),
            ("GET", "/endpoint-f/metrics", product_2.get_endpoint_f_metrics),
            ("GET", "/endpoint-g", product_2.get_endpoint_g),
        ]
        
        # Pre-compile route patterns for performance
        self._compiled_routes = [
            (method.upper(), self._compile_pattern(pattern), handler)
            for method, pattern, handler in self.routes
        ]
    
    @staticmethod
    def _compile_pattern(pattern: str) -> re.Pattern:
        """
        Convert a path pattern like '/accounts/{id}' into a regex.
        """
        regex_pattern = re.sub(
            r'\{(\w+)\}',
            r'(?P<\1>[^/]+)',
            pattern
        )
        return re.compile(f"^{regex_pattern}$")
    
    def route(self, event: Dict[str, Any], context: Any) -> Any:
        """
        Route the incoming request to the correct handler.

        Handles two different event structures:
          - API Gateway:         event["httpMethod"] / event["path"]
          - Lambda Function URL: event["requestContext"]["http"]["method"] / ["path"]
        """
        # Authorization check (applies to ALL endpoints)
        validate_authorization(event)
        
        # ------------------------------------------------------------------
        # Extract method and path
        # Lambda Function URL and API Gateway use different event structures
        # ------------------------------------------------------------------
        http_context = event.get("requestContext", {}).get("http", {})

        method = (
            event.get("httpMethod")                 # API Gateway
            or http_context.get("method")           # Lambda Function URL
            or "GET"
        ).upper()

        path = (
            event.get("path")                       # API Gateway
            or http_context.get("path")             # Lambda Function URL
            or "/"
        )
        
        # Find matching route
        for route_method, route_regex, handler in self._compiled_routes:
            if route_method != method:
                continue
            
            match = route_regex.match(path)
            if match:
                # Extract path parameters and inject into event
                path_params = match.groupdict()
                event["pathParameters"] = {
                    **(event.get("pathParameters") or {}),
                    **path_params,
                }
                
                return handler(event, context)
        
        raise NotFoundError(f"Route not found: {method} {path}")
    
    def list_routes(self) -> list:
        """Return all registered routes (useful for debugging/docs endpoint)."""
        return [(method, pattern) for method, pattern, _ in self.routes]