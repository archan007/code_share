/* =============================================================================
   SNOWPIPE SETUP SCRIPT — S3 AUTO-INGEST
   -----------------------------------------------------------------------------
   Purpose : Create all prerequisites for Snowpipe (integration, stage,
             file formats, table, pipe) for continuous ingestion from S3.
   Prereqs : - S3 bucket already provisioned
             - AWS team has provided: IAM role, IAM role ARN,
               access key id, secret access key
             - Note: For storage integrations, only the IAM role ARN is
               needed. The access key / secret key would only be used for
               the older (and less secure) direct-credential stage pattern.
   Run as  : Mix of ACCOUNTADMIN (integration) and SYSADMIN (objects).
   =============================================================================
*/


/* -----------------------------------------------------------------------------
   STEP 1 — CREATE DATABASE AND SCHEMA (skip if already existing)
   -----------------------------------------------------------------------------
*/
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS RAW_DB;
CREATE SCHEMA   IF NOT EXISTS RAW_DB.LANDING;

USE DATABASE RAW_DB;
USE SCHEMA   LANDING;


/* -----------------------------------------------------------------------------
   STEP 2 — CREATE STORAGE INTEGRATION
   -----------------------------------------------------------------------------
   This is the trust bridge between Snowflake and AWS. Only ACCOUNTADMIN can
   create it. Replace placeholders with your actual values.

   IMPORTANT: STORAGE_AWS_ROLE_ARN is the IAM ROLE ARN your AWS team gave
   you — NOT the access key / secret. Snowflake uses STS AssumeRole against
   this role, so the access key + secret are not needed for this pattern.
*/
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_int_prod
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<your-aws-account-id>:role/<your-role-name>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/path1/',
                               's3://your-bucket-name/path2/')
  -- STORAGE_BLOCKED_LOCATIONS is optional; useful for keeping sensitive prefixes out
  STORAGE_BLOCKED_LOCATIONS = ('s3://your-bucket-name/sensitive/');


/* -----------------------------------------------------------------------------
   STEP 3 — GET SNOWFLAKE'S IAM USER DETAILS AND UPDATE AWS TRUST POLICY
   -----------------------------------------------------------------------------
   Run DESC and capture these two values from the output:
       - STORAGE_AWS_IAM_USER_ARN
       - STORAGE_AWS_EXTERNAL_ID

   Send them to your AWS team so they can update the trust relationship
   on the IAM role to allow Snowflake's IAM user to assume it. Without
   this, the stage will throw an access-denied error at LIST/COPY time.
*/
DESC INTEGRATION s3_int_prod;


/* -----------------------------------------------------------------------------
   STEP 4 — GRANT USAGE ON THE INTEGRATION
   -----------------------------------------------------------------------------
   Grant to whichever role will own / reference the stages.
*/
GRANT USAGE ON INTEGRATION s3_int_prod TO ROLE SYSADMIN;


/* -----------------------------------------------------------------------------
   STEP 5 — CREATE FILE FORMATS
   -----------------------------------------------------------------------------
   Create one per file type you expect to land. File formats are reusable
   across any number of stages, pipes, and COPY statements.
*/
USE ROLE   SYSADMIN;
USE SCHEMA RAW_DB.LANDING;

-- CSV
CREATE OR REPLACE FILE FORMAT ff_csv
  TYPE                         = CSV
  FIELD_DELIMITER              = ','
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL          = TRUE
  COMPRESSION                  = AUTO;

-- JSON
CREATE OR REPLACE FILE FORMAT ff_json
  TYPE              = JSON
  STRIP_OUTER_ARRAY = TRUE
  COMPRESSION       = AUTO;

-- Parquet
CREATE OR REPLACE FILE FORMAT ff_parquet
  TYPE = PARQUET;


/* -----------------------------------------------------------------------------
   STEP 6 — CREATE EXTERNAL STAGE
   -----------------------------------------------------------------------------
   The stage references the integration and a specific S3 location. One
   stage can back multiple pipes as long as each pipe reads from a
   distinct sub-prefix (don't let two pipes watch overlapping paths, or
   you'll double-load).
*/
CREATE OR REPLACE STAGE stg_s3_landing
  STORAGE_INTEGRATION = s3_int_prod
  URL                 = 's3://your-bucket-name/path1/'
  FILE_FORMAT         = ff_csv;   -- default; can be overridden per COPY

-- Quick sanity check — if this lists your files, the integration + trust
-- policy are wired up correctly.
LIST @stg_s3_landing;


/* -----------------------------------------------------------------------------
   STEP 7 — CREATE TARGET TABLE
   -----------------------------------------------------------------------------
   Example schema — adjust to your actual data.
*/
CREATE OR REPLACE TABLE orders_raw (
  order_id    STRING,
  customer_id STRING,
  order_date  TIMESTAMP,
  amount      NUMBER(12,2),
  loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


/* -----------------------------------------------------------------------------
   STEP 8 — CREATE THE SNOWPIPE
   -----------------------------------------------------------------------------
   AUTO_INGEST = TRUE tells Snowflake to listen on an SQS queue for S3
   event notifications. ON_ERROR = 'CONTINUE' skips bad files instead of
   aborting the load — pick the behaviour that fits your data quality
   tolerance.
*/
CREATE OR REPLACE PIPE pipe_orders_ingest
  AUTO_INGEST = TRUE
AS
COPY INTO orders_raw (order_id, customer_id, order_date, amount)
FROM (
  SELECT $1, $2, $3, $4
  FROM @stg_s3_landing/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_csv')
ON_ERROR    = 'CONTINUE';


/* -----------------------------------------------------------------------------
   STEP 9 — GET THE SQS ARN AND CONFIGURE S3 EVENT NOTIFICATIONS
   -----------------------------------------------------------------------------
   SHOW PIPES returns a 'notification_channel' column — this is the SQS
   ARN your pipe listens on. Give it to your AWS team so they can add an
   S3 event notification on the bucket/prefix:
       Event type : s3:ObjectCreated:*
       Destination: SQS queue (the ARN above)
   Once that's in place, new files landing in the prefix will fire the
   pipe automatically.
*/
SHOW PIPES;


/* -----------------------------------------------------------------------------
   STEP 10 — VERIFY THE PIPE IS HEALTHY AND LOADING
   -----------------------------------------------------------------------------
*/
-- Current status (executionState should be RUNNING)
SELECT SYSTEM$PIPE_STATUS('pipe_orders_ingest');

-- Recent load history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
      TABLE_NAME => 'orders_raw',
      START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP())
));


/* =============================================================================
   REUSE NOTES — INTEGRATION / STAGE / FILE FORMAT ACROSS MULTIPLE PIPES
   -----------------------------------------------------------------------------

   STORAGE INTEGRATION
     Reusable. One integration per AWS account / IAM role is the standard
     pattern. The same integration backs many stages as long as the target
     S3 locations are covered by STORAGE_ALLOWED_LOCATIONS. Creating a
     new integration per pipe is unnecessary overhead and means more
     trust-policy coordination with AWS.

     Gotcha: CREATE OR REPLACE on an integration rotates the external ID
     and breaks the AWS trust policy until it's updated. Use
     ALTER STORAGE INTEGRATION to modify in place.

   STAGE
     Reusable, with a caveat. One stage can back multiple pipes provided
     each pipe's COPY reads from a DISTINCT sub-prefix
     (e.g. @stg/orders/, @stg/customers/). Don't have two pipes watching
     overlapping paths — both will react to the same S3 event and you'll
     get duplicate loads.

     Two common patterns:
       (a) One broad stage at the bucket root, pipes scoped by sub-prefix.
       (b) One stage per logical dataset — easier when permissions or
           lifecycle differ per dataset.

   FILE FORMAT
     Fully reusable. Reference by name from any number of stages, pipes,
     and COPY statements.
   =============================================================================
*/

Please update the trust policy on IAM role <role-name> with the JSON below.
Values from Snowflake:

IAM User ARN: <paste STORAGE_AWS_IAM_USER_ARN here>
External ID: <paste STORAGE_AWS_EXTERNAL_ID here>

Trust policy JSON:
json{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "<IAM User ARN>" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "<External ID>" }
      }
    }
  ]
}