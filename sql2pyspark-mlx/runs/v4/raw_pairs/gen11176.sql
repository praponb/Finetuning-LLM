WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, attr_value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT k, value
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) t AS value
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(value, 1, 2) AS value_prefix
    FROM exploded
    WHERE value IS NOT NULL
      AND k IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(value, 'Unknown') AS value_filled,
        LENGTH(value) AS value_length
    FROM filtered
    WHERE value IN ('BUNDLE', 'VIP', 'SPRING10')
)
-- Aggregate with conditional metrics
SELECT
    k,
    value,
    SORT_ARRAY(COLLECT_SET(value)) AS value_list,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN value = 'VIP' THEN 1 END) AS vip_count,
    RANK() OVER (PARTITION BY k ORDER BY COUNT(*) DESC) AS event_count_rank
FROM cleaned
GROUP BY 1, 2
