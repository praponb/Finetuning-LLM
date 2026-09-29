WITH matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM subscriptions
    EXCEPT ALL
    SELECT customer_id
    FROM subscriptions
    WHERE monthly_fee > 180
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        PERCENT_RANK() OVER w_ordered AS monthly_fee_percent_rank,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_rank
    FROM verified
    WINDOW w_ordered AS (PARTITION BY status ORDER BY monthly_fee NULLS LAST)
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.name,
        d.credit_limit
    FROM with_windows s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.email,
        b.country,
        b.segment
    FROM with_dims a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(monthly_fee_percent_rank, monthly_fee) AS least_monthly_fee_percent_rank_monthly_fee,
        COALESCE(country, 'NONE') AS resolved_country
    FROM enriched
    WHERE end_date > TO_DATE('2023-07-19')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_percent_rank,
        monthly_fee_rank,
        name,
        country,
        segment,
        least_monthly_fee_percent_rank_monthly_fee,
        resolved_country,
        CASE WHEN monthly_fee_percent_rank >= 0.63 THEN 'High' WHEN monthly_fee_percent_rank >= 0.32 THEN 'Medium' ELSE 'Low' END AS monthly_fee_percent_rank_level,
        CONCAT(country, ' ', email) AS country_email,
        COALESCE(segment, 'UNKNOWN') AS segment_clean
    FROM filtered
    WHERE end_date >= DATE '2023-07-08'
)
SELECT
    customer_id,
    plan,
    monthly_fee,
    status,
    monthly_fee_percent_rank,
    monthly_fee_rank,
    name,
    country,
    segment,
    CASE WHEN plan RLIKE '^(premium|standard)$' THEN 'EXTENDED' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS plan_zone
FROM base
ORDER BY monthly_fee_percent_rank DESC NULLS FIRST
