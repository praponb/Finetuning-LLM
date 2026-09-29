WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN device IN ('tablet', 'mobile') THEN 'Group A' ELSE 'Other' END AS device_group,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-09-28 09:00:00'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, browser
    FROM base
    INTERSECT
    SELECT id, browser
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 11 AND 15
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id AND s.browser = k.browser
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.device_group,
        a.days_since_started_at,
        d.segment,
        d.credit_limit,
        d.signup_date
    FROM not_flagged a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device_group,
        days_since_started_at,
        segment,
        RANK() OVER (PARTITION BY device ORDER BY days_since_started_at NULLS FIRST) AS days_since_started_at_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank
    FROM joined
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_group,
        days_since_started_at,
        segment,
        days_since_started_at_rank,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY credit_limit_share DESC NULLS FIRST) AS credit_limit_share_percent_rank,
        ROUND(credit_limit_dense_rank / SUM(credit_limit_dense_rank) OVER (PARTITION BY segment), 4) AS credit_limit_dense_rank_share,
        RANK() OVER (PARTITION BY customer_id, segment ORDER BY credit_limit_share DESC) AS credit_limit_share_rank,
        CUME_DIST() OVER (ORDER BY days_since_started_at_rank DESC NULLS FIRST) AS days_since_started_at_rank_cume_dist
    FROM enriched
),
metrics_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_group,
        days_since_started_at,
        days_since_started_at_rank,
        DENSE_RANK() OVER w_grp AS credit_limit_dense_rank_share_dense_rank,
        RANK() OVER (PARTITION BY segment ORDER BY credit_limit_share_percent_rank DESC) AS credit_limit_share_percent_rank_rank,
        STDDEV_SAMP(days_since_started_at_rank) OVER w_ordered AS segment_stddev_samp_days_since_started_at_rank,
        PERCENT_RANK() OVER w_recent AS credit_limit_share_rank_percent_rank
    FROM metrics
    WINDOW w_grp AS (PARTITION BY segment ORDER BY credit_limit_dense_rank_share DESC), w_ordered AS (PARTITION BY segment), w_recent AS (PARTITION BY segment ORDER BY credit_limit_share_rank NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit_share_rank_percent_rank >= 0.89 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_share_rank_percent_rank_tier,
    PERCENTILE_APPROX(credit_limit_share_percent_rank_rank, 0.95) AS credit_limit_share_percent_rank_rank_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(days_since_started_at) AS min_days_since_started_at
FROM metrics_sessions
GROUP BY CUBE (CASE WHEN credit_limit_share_rank_percent_rank >= 0.89 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY distinct_ids DESC NULLS FIRST
