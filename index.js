/**
 * CloudFront Function — Viewer Request
 *
 * Testing phase: validates Bearer token format only
 * Adds x-internal-verified header for WAF passthrough
 */

function handler(event) {
  var request = event.request;
  var headers = request.headers;

  // ----------------------------------------------------------------
  // Extract Authorization header
  // ----------------------------------------------------------------
  var authHeader = headers["authorization"]
    ? headers["authorization"].value
    : "";

  // ----------------------------------------------------------------
  // No auth header — block immediately
  // ----------------------------------------------------------------
  if (!authHeader) {
    return {
      statusCode: 401,
      statusDescription: "Unauthorized",
      headers: {
        "content-type": { value: "application/json" },
        "access-control-allow-origin": { value: "*" },
      },
      body: JSON.stringify({ message: "Missing authorization header" }),
    };
  }

  // ----------------------------------------------------------------
  // Wrong format — block
  // ----------------------------------------------------------------
  if (!authHeader.startsWith("Bearer ")) {
    return {
      statusCode: 401,
      statusDescription: "Unauthorized",
      headers: {
        "content-type": { value: "application/json" },
        "access-control-allow-origin": { value: "*" },
      },
      body: JSON.stringify({
        message: "Invalid format. Expected: Bearer <token>",
      }),
    };
  }

  // ----------------------------------------------------------------
  // Empty token — block
  // ----------------------------------------------------------------
  var token = authHeader.slice(7).trim();
  if (!token) {
    return {
      statusCode: 401,
      statusDescription: "Unauthorized",
      headers: {
        "content-type": { value: "application/json" },
        "access-control-allow-origin": { value: "*" },
      },
      body: JSON.stringify({ message: "Empty token" }),
    };
  }

  // ----------------------------------------------------------------
  // Auth passed — add internal header for WAF and forward request
  // ----------------------------------------------------------------
  request.headers["x-internal-verified"] = { value: "true" };

  return request;
}
