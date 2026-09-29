WITH with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.status,
        latest.subscription_plan
    FROM customers s
    JOIN LATERAL (
            SELECT c.status, c.plan AS subscription_plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment ORDER BY age NULLS LAST) AS rank_in_group
        FROM with_latest
    ) t
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, segment
    FROM ranked
    EXCEPT DISTINCT
    SELECT city, segment
    FROM customers
    WHERE MONTH(signup_date) IN (1, 2, 11)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN matched_keys k ON s.city = k.city AND s.segment = k.segment
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(is_active AS INT) AS is_active_flag,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM reconciled
    WHERE status IN ('ACTIVE', 'CANCELLED')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        status,
        subscription_plan,
        rank_in_group,
        is_active_flag,
        resolved_segment,
        ROUND(rank_in_group * 1.15, 2) AS adjusted_rank_in_group
    FROM valid
    WHERE signup_date >= TO_DATE('2024-04-10')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN subscription_plan IN ('basic', 'standard') THEN 'Group A' ELSE 'Other' END AS subscription_plan_group,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM base
    WHERE signup_date > TO_DATE('2023-04-27')
      AND subscription_plan IS NOT NULL
      AND is_active_flag <> 36
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        status,
        subscription_plan,
        rank_in_group,
        is_active_flag,
        resolved_segment,
        adjusted_rank_in_group,
        subscription_plan_group,
        resolved_city,
        SUM(adjusted_rank_in_group) OVER (PARTITION BY status) AS status_sum_adjusted_rank_in_group,
        RANK() OVER (ORDER BY is_active_flag DESC) AS is_active_flag_rank,
        PERCENT_RANK() OVER (ORDER BY is_active_flag NULLS LAST) AS is_active_flag_percent_rank
    FROM filtered
)
-- Ranking and gap features
SELECT
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    status,
    subscription_plan,
    rank_in_group,
    is_active_flag,
    resolved_segment,
    adjusted_rank_in_group,
    subscription_plan_group,
    resolved_city,
    status_sum_adjusted_rank_in_group,
    is_active_flag_rank,
    is_active_flag_percent_rank,
    MIN(is_active_flag_percent_rank) OVER (PARTITION BY country) AS country_min_is_active_flag_percent_rank,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city, subscription_plan), 4) AS credit_limit_share,
    DENSE_RANK() OVER (ORDER BY is_active_flag_percent_rank) AS is_active_flag_percent_rank_dense_rank,
    CUME_DIST() OVER (PARTITION BY city ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_cume_dist
FROM scored;
