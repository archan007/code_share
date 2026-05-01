-- =============================================================================
-- Stored Procedure: sp_account_summary
-- Schema:           GOLD_C360
-- Description:      Returns paginated account summary data with filtering,
--                   sorting, and free-text search. Each row includes
--                   TOTAL_COUNT for pagination metadata.
--
-- Parameters (in order, matching Lambda call signature):
--   P_PAGE              NUMBER        Page number (1-indexed)
--   P_LIMIT             NUMBER        Records per page (max 100)
--   P_SEARCH            VARCHAR       Free-text search (name, sector, csm, am, region, product)
--   P_ACCOUNT_MGR_KEY   VARCHAR       Account manager slug filter
--   P_REGION_KEY        VARCHAR       Region slug filter
--   P_SEGMENT_KEY       VARCHAR       Segment slug filter
--   P_PRODUCT           VARCHAR       Primary product filter
--   P_USAGE_TREND       VARCHAR       Usage trend filter
--   P_RENEWAL_DAYS_MIN  NUMBER        Minimum days to renewal (inclusive)
--   P_RENEWAL_DAYS_MAX  NUMBER        Maximum days to renewal (inclusive)
--   P_STATUS            VARCHAR       Account health status filter
--   P_SORT              VARCHAR       Sort field
--   P_SORT_ORDER        VARCHAR       Sort direction (asc / desc)
--
-- Returns: TABLE with all ACCOUNT_SUMMARY_VW columns + TOTAL_COUNT
--
-- Usage:
--   CALL GOLD_C360.sp_account_summary(1, 25, NULL, NULL, NULL, NULL,
--                                     NULL, NULL, NULL, NULL, NULL,
--                                     'default', 'desc');
-- =============================================================================

CREATE OR REPLACE PROCEDURE GOLD_C360.sp_account_summary(
    P_PAGE              NUMBER,
    P_LIMIT             NUMBER,
    P_SEARCH            VARCHAR,
    P_ACCOUNT_MGR_KEY   VARCHAR,
    P_REGION_KEY        VARCHAR,
    P_SEGMENT_KEY       VARCHAR,
    P_PRODUCT           VARCHAR,
    P_USAGE_TREND       VARCHAR,
    P_RENEWAL_DAYS_MIN  NUMBER,
    P_RENEWAL_DAYS_MAX  NUMBER,
    P_STATUS            VARCHAR,
    P_SORT              VARCHAR,
    P_SORT_ORDER        VARCHAR
)
RETURNS TABLE (
    ID                      NUMBER,
    NAME                    VARCHAR,
    INITIALS                VARCHAR,
    SECTOR                  VARCHAR,
    SUBSCRIPTION_TYPE       VARCHAR,
    ANNUAL_CONTRACT_VALUE   NUMBER,
    RISK_PERCENTAGE         NUMBER,
    LAST_TOUCH_DAYS         NUMBER,
    RENEWAL_DAYS            NUMBER,
    CSM_NAME                VARCHAR,
    ACCOUNT_MANAGER_NAME    VARCHAR,
    ACCOUNT_MANAGER_KEY     VARCHAR,
    REGION                  VARCHAR,
    REGION_KEY              VARCHAR,
    SEGMENT                 VARCHAR,
    SEGMENT_KEY             VARCHAR,
    STATUS                  VARCHAR,
    FLAGGED_BY              VARCHAR,
    RISK_REASON             VARCHAR,
    USAGE_TREND             VARCHAR,
    EXPANSION_VALUE         NUMBER,
    PRIMARY_PRODUCT         VARCHAR,
    TOTAL_COUNT             NUMBER
)
LANGUAGE SQL
AS
$$
DECLARE

    -- Pagination
    v_offset    NUMBER;
    v_limit     NUMBER;

    -- Resolved sort column (guards against SQL injection via CASE)
    v_sort_col  VARCHAR;

    -- Final dynamic SQL statement
    v_sql       VARCHAR;

BEGIN

    -- -------------------------------------------------------------------------
    -- 1. Resolve pagination offset
    -- -------------------------------------------------------------------------
    v_offset := (P_PAGE - 1) * P_LIMIT;
    v_limit  := P_LIMIT;

    -- -------------------------------------------------------------------------
    -- 2. Resolve sort column
    --    Using CASE here deliberately — never interpolate raw user input
    --    directly as a column name. CASE restricts it to known safe values.
    -- -------------------------------------------------------------------------
    v_sort_col := CASE LOWER(P_SORT)
        WHEN 'name'         THEN 'NAME'
        WHEN 'risk-value'   THEN 'RISK_PERCENTAGE'
        WHEN 'acv'          THEN 'ANNUAL_CONTRACT_VALUE'
        WHEN 'renewal-date' THEN 'RENEWAL_DAYS'
        WHEN 'last-touch'   THEN 'LAST_TOUCH_DAYS'
        ELSE 'RISK_PERCENTAGE'  -- default
    END;

    -- -------------------------------------------------------------------------
    -- 3. Build dynamic SQL
    --
    --    Pattern:
    --      filtered_data CTE  → applies all WHERE filters
    --      counted_data CTE   → attaches TOTAL_COUNT (one COUNT over full set)
    --      Final SELECT       → applies ORDER BY + LIMIT + OFFSET
    --
    --    Why CTEs instead of a subquery?
    --    COUNT(*) OVER() on a large filtered set is computed once and attached
    --    to every row — cheaper than a separate COUNT query.
    -- -------------------------------------------------------------------------
    v_sql := '
    WITH filtered_data AS (
        SELECT
            ID,
            NAME,
            INITIALS,
            SECTOR,
            SUBSCRIPTION_TYPE,
            ANNUAL_CONTRACT_VALUE,
            RISK_PERCENTAGE,
            LAST_TOUCH_DAYS,
            RENEWAL_DAYS,
            CSM_NAME,
            ACCOUNT_MANAGER_NAME,
            ACCOUNT_MANAGER_KEY,
            REGION,
            REGION_KEY,
            SEGMENT,
            SEGMENT_KEY,
            STATUS,
            FLAGGED_BY,
            RISK_REASON,
            USAGE_TREND,
            EXPANSION_VALUE,
            PRIMARY_PRODUCT
        FROM GOLD_C360.ACCOUNT_SUMMARY_VW
        WHERE 1=1
    ';

    -- -------------------------------------------------------------------------
    -- 4. Conditionally append each filter
    --    NULL check means "only filter if parameter was actually passed"
    -- -------------------------------------------------------------------------

    -- Free-text search across key human-readable fields
    -- ILIKE is case-insensitive in Snowflake
    IF (P_SEARCH IS NOT NULL AND P_SEARCH != '') THEN
        v_sql := v_sql || '
            AND (
                NAME                 ILIKE ''%' || P_SEARCH || '%''
                OR SECTOR            ILIKE ''%' || P_SEARCH || '%''
                OR CSM_NAME          ILIKE ''%' || P_SEARCH || '%''
                OR ACCOUNT_MANAGER_NAME ILIKE ''%' || P_SEARCH || '%''
                OR REGION            ILIKE ''%' || P_SEARCH || '%''
                OR PRIMARY_PRODUCT   ILIKE ''%' || P_SEARCH || '%''
            )';
    END IF;

    IF (P_ACCOUNT_MGR_KEY IS NOT NULL AND P_ACCOUNT_MGR_KEY != '') THEN
        v_sql := v_sql || '
            AND ACCOUNT_MANAGER_KEY = ''' || P_ACCOUNT_MGR_KEY || '''';
    END IF;

    IF (P_REGION_KEY IS NOT NULL AND P_REGION_KEY != '') THEN
        v_sql := v_sql || '
            AND REGION_KEY = ''' || P_REGION_KEY || '''';
    END IF;

    IF (P_SEGMENT_KEY IS NOT NULL AND P_SEGMENT_KEY != '') THEN
        v_sql := v_sql || '
            AND SEGMENT_KEY = ''' || P_SEGMENT_KEY || '''';
    END IF;

    IF (P_PRODUCT IS NOT NULL AND P_PRODUCT != '') THEN
        v_sql := v_sql || '
            AND PRIMARY_PRODUCT = ''' || P_PRODUCT || '''';
    END IF;

    IF (P_USAGE_TREND IS NOT NULL AND P_USAGE_TREND != '') THEN
        v_sql := v_sql || '
            AND USAGE_TREND = ''' || P_USAGE_TREND || '''';
    END IF;

    IF (P_RENEWAL_DAYS_MIN IS NOT NULL) THEN
        v_sql := v_sql || '
            AND RENEWAL_DAYS >= ' || P_RENEWAL_DAYS_MIN;
    END IF;

    IF (P_RENEWAL_DAYS_MAX IS NOT NULL) THEN
        v_sql := v_sql || '
            AND RENEWAL_DAYS <= ' || P_RENEWAL_DAYS_MAX;
    END IF;

    IF (P_STATUS IS NOT NULL AND P_STATUS != '') THEN
        v_sql := v_sql || '
            AND STATUS = ''' || P_STATUS || '''';
    END IF;

    -- -------------------------------------------------------------------------
    -- 5. Attach total count, apply sort and pagination
    -- -------------------------------------------------------------------------
    v_sql := v_sql || '
    ),
    counted_data AS (
        SELECT
            *,
            COUNT(*) OVER () AS TOTAL_COUNT
        FROM filtered_data
    )
    SELECT
        ID,
        NAME,
        INITIALS,
        SECTOR,
        SUBSCRIPTION_TYPE,
        ANNUAL_CONTRACT_VALUE,
        RISK_PERCENTAGE,
        LAST_TOUCH_DAYS,
        RENEWAL_DAYS,
        CSM_NAME,
        ACCOUNT_MANAGER_NAME,
        ACCOUNT_MANAGER_KEY,
        REGION,
        REGION_KEY,
        SEGMENT,
        SEGMENT_KEY,
        STATUS,
        FLAGGED_BY,
        RISK_REASON,
        USAGE_TREND,
        EXPANSION_VALUE,
        PRIMARY_PRODUCT,
        TOTAL_COUNT
    FROM counted_data
    ORDER BY ' || v_sort_col || ' ' || UPPER(P_SORT_ORDER) || '
    LIMIT  ' || v_limit  || '
    OFFSET ' || v_offset;

    -- -------------------------------------------------------------------------
    -- 6. Execute and return
    -- -------------------------------------------------------------------------
    RETURN TABLE(EXECUTE IMMEDIATE v_sql);

END;
$$;


-- =============================================================================
-- PERMISSIONS
-- Grant execute to the API role for each environment.
-- Run the appropriate block per environment.
-- =============================================================================

-- DEV
GRANT USAGE ON PROCEDURE GOLD_C360.sp_account_summary(
    NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
    VARCHAR, VARCHAR, NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR
) TO ROLE API_ROLE_DEV;

-- UAT
GRANT USAGE ON PROCEDURE GOLD_C360.sp_account_summary(
    NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
    VARCHAR, VARCHAR, NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR
) TO ROLE API_ROLE_UAT;

-- PROD
GRANT USAGE ON PROCEDURE GOLD_C360.sp_account_summary(
    NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
    VARCHAR, VARCHAR, NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR
) TO ROLE API_ROLE_PROD;


-- =============================================================================
-- SMOKE TESTS
-- Run these in a Snowflake worksheet to verify the SP after creation.
-- =============================================================================

-- Test 1: No filters — should return first page of all accounts
CALL GOLD_C360.sp_account_summary(
    1, 25, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    'default', 'desc'
);

-- Test 2: Free-text search
CALL GOLD_C360.sp_account_summary(
    1, 25, 'Goldman', NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    'default', 'desc'
);

-- Test 3: Status filter + sort by ACV descending
CALL GOLD_C360.sp_account_summary(
    1, 10, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, 'critical',
    'acv', 'desc'
);

-- Test 4: Renewal window filter (next 90 days)
CALL GOLD_C360.sp_account_summary(
    1, 25, NULL, NULL, NULL, NULL,
    NULL, NULL, 0, 90, NULL,
    'renewal-date', 'asc'
);

-- Test 5: Region + segment + product combined
CALL GOLD_C360.sp_account_summary(
    1, 25, NULL, NULL, 'north-america', 'enterprise',
    'CreditSights', NULL, NULL, NULL, NULL,
    'risk-value', 'desc'
);

-- Test 6: Page 2 — verify pagination and TOTAL_COUNT is consistent
CALL GOLD_C360.sp_account_summary(
    2, 10, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    'default', 'desc'
);

-- Test 7: All filters combined
CALL GOLD_C360.sp_account_summary(
    1, 25, 'Goldman', 'sarah', 'north-america', 'enterprise',
    'CreditSights', 'declining', 0, 90, 'critical',
    'risk-value', 'desc'
);