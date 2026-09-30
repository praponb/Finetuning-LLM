WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(age AS DOUBLE) AS age_str,
        COALESCE(city, 'NONE') AS city_filled
    FROM customers
    WHERE signup_date >= TO_DATE('2023-07-21')
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.segment,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.age_str,
        latest.subscription_plan
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.started_at,
        top1.device
    FROM with_last s
    JOIN LATERAL (
            SELECT c.started_at, c.device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, segment
    FROM latest_match
    EXCEPT
    SELECT id, segment
    FROM customers
    WHERE segment IN ('SMB', 'Consumer', 'Corporate')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN matched_keys k ON s.id = k.id AND s.segment = k.segment
),
valid_customers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        segment,
        is_active,
        age_str,
        started_at,
        device,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek,
        INITCAP(TRIM(segment)) AS segment_title
    FROM kept_rows
    WHERE subscription_plan IS NOT NULL
      AND device IS NOT NULL
)
SELECT
    name,
    segment,
    is_active,
    age_str,
    started_at,
    device,
    signup_date_dayofweek,
    segment_title
FROM valid_customers
