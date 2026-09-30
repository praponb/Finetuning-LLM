WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(device) AS device_upper,
        TO_DATE(started_at) AS started_at_date,
        COALESCE(country, 'NONE') AS resolved_country
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-04-03 17:00:00'
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, d.name
    FROM filtered e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, device
    FROM enriched
    INTERSECT
    SELECT customer_id, device
    FROM sessions
    WHERE browser IN ('Edge', 'Safari')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.device = k.device
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.name AS customer_name,
        d.country AS customer_country
    FROM reconciled src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
enriched_sessions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.segment,
        c.credit_limit
    FROM with_dims e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_cume_dist,
        CUME_DIST() OVER (PARTITION BY customer_country ORDER BY credit_limit) AS credit_limit_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY segment, customer_country ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_cume_dist_3
    FROM enriched_sessions
),
with_windows_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(credit_limit_cume_dist_2) OVER (PARTITION BY segment) AS segment_sum_credit_limit_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY segment ORDER BY credit_limit_cume_dist_2) AS credit_limit_cume_dist_2_cume_dist,
        SUM(credit_limit_cume_dist_3) OVER (PARTITION BY segment) AS segment_sum_credit_limit_cume_dist_3
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', started_at_date) AS year_start,
    DATE_TRUNC('MONTH', started_at_date) AS activity_month,
    MAX(started_at) AS last_started_at,
    APPROX_PERCENTILE(credit_limit, 0.9) AS credit_limit_p90,
    AVG(credit_limit_cume_dist_2_cume_dist) FILTER (WHERE customer_country = 'US') AS us_avg_credit_limit_cume_dist_2_cume_dist,
    COUNT(DISTINCT browser) AS distinct_browsers,
    RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', started_at_date) ORDER BY COUNT(DISTINCT browser) DESC) AS distinct_browsers_rank
FROM with_windows_sessions
GROUP BY DATE_TRUNC('YEAR', started_at_date), DATE_TRUNC('MONTH', started_at_date)
HAVING APPROX_PERCENTILE(credit_limit, 0.9) > 0;
