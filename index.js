/**
 * Lambda@Edge — Viewer Request
 *
 * Testing phase: validates Bearer token format only
 * Adds x-internal-verified header for WAF passthrough
 */

export const handler = async (event) => {
  const request = event.Records[0].cf.request;
  const headers = request.headers;

  // ----------------------------------------------------------------
  // LOG incoming request details (visible in CloudWatch)
  // ----------------------------------------------------------------
  console.log("=== Lambda@Edge Viewer Request ===");
  console.log("URI     :", request.uri);
  console.log("Method  :", request.method);
  console.log("Headers :", JSON.stringify(headers, null, 2));

  // ----------------------------------------------------------------
  // Extract Authorization header
  // CloudFront lowercases all header names
  // ----------------------------------------------------------------
  const authHeader = headers["authorization"]?.[0]?.value || "";
  console.log("Auth header received:", authHeader ? "YES" : "NO");

  // ----------------------------------------------------------------
  // Validate token
  // ----------------------------------------------------------------
  if (!authHeader) {
    console.log("BLOCKED — missing authorization header");
    return buildUnauthorizedResponse("Missing authorization header");
  }

  if (!authHeader.startsWith("Bearer ")) {
    console.log("BLOCKED — invalid format");
    return buildUnauthorizedResponse(
      "Invalid format. Expected: Bearer <token>",
    );
  }

  const token = authHeader.slice(7).trim();

  if (!token) {
    console.log("BLOCKED — empty token");
    return buildUnauthorizedResponse("Empty token");
  }

  // ----------------------------------------------------------------
  // Auth passed — add internal header so WAF allows the request
  // ----------------------------------------------------------------
  console.log("PASSED — forwarding request to origin");

  request.headers["x-internal-verified"] = [
    {
      key: "x-internal-verified",
      value: "true",
    },
  ];

  return request;
};

/**
 * Builds a 401 response — Lambda is never invoked
 */
function buildUnauthorizedResponse(reason) {
  return {
    status: "401",
    statusDescription: "Unauthorized",
    headers: {
      "content-type": [
        {
          key: "Content-Type",
          value: "application/json",
        },
      ],
      "access-control-allow-origin": [
        {
          key: "Access-Control-Allow-Origin",
          value: "*",
        },
      ],
    },
    body: JSON.stringify({ message: reason }),
  };
}
