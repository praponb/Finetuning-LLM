WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date_2
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, p.segment
    FROM base s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.start_date,
        s.days_since_end_date,
        s.days_since_end_date_2,
        s.segment,
        p.credit_limit,
        p.name,
        p.country
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(name, ', ', segment) AS name_segment,
        COALESCE(credit_limit, 0) AS credit_limit_filled
    FROM joined
    WHERE start_date >= DATE '2023-05-27'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit_filled DESC) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(days_since_end_date_2, days_since_end_date) AS least_days_since_end_date_2_days_since_end_date,
        COALESCE(segment, 'NONE') AS resolved_segment
    FROM deduped
    WHERE start_date > DATE '2023-03-11'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN days_since_end_date >= 66 THEN 'LARGE' ELSE 'SMALL' END AS days_since_end_date_tier,
        customer_id,
        country,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
        SIZE(COLLECT_SET(segment)) AS num_segments
    FROM base_subscriptions
    GROUP BY days_since_end_date_tier, customer_id, country
)
SELECT
    days_since_end_date_tier,
    customer_id,
    country,
    segment_list
FROM totals
ORDER BY days_since_end_date_tier
