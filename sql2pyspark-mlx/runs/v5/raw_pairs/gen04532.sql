WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, v
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.credit_limit,
        b.name
    FROM exploded s
    LEFT JOIN customers b
        ON s.customer_id = b.id
    WHERE b.name IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        attr_key,
        v,
        name,
        PERCENT_RANK() OVER (ORDER BY credit_limit NULLS LAST) AS credit_limit_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_percent_rank_2
    FROM joined
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        attr_key,
        v,
        GREATEST(credit_limit_percent_rank_2, credit_limit_percent_rank) AS greatest_credit_limit_percent_rank_2_credit_limit_percent_rank,
        COALESCE(attr_key, 'DIRECT') AS attr_key_clean
    FROM with_windows
    WHERE attr_key IS NOT NULL
),
stats AS (
    -- Grouped metrics
    SELECT
        attr_key,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
        COUNT(*) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_rows,
        COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
        COUNT_IF(attr_key = 'gift') AS gift_events,
        PERCENTILE_APPROX(greatest_credit_limit_percent_rank_2_credit_limit_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS greatest_credit_limit_percent_rank_2_credit_limit_percent_rank_quartiles,
        COUNT_IF(attr_key = 'gift') AS gift_events_2
    FROM valid
    GROUP BY attr_key
)
SELECT
    attr_key,
    num_attr_keys,
    gift_rows,
    gift_events,
    greatest_credit_limit_percent_rank_2_credit_limit_percent_rank_quartiles
FROM stats
