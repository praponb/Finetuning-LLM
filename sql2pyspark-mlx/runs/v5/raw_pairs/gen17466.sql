WITH flattened AS (
    SELECT
        id,
        created_at,
        value
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS value
),
kept_keys AS (
    SELECT id
    FROM flattened
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
kept_rows AS (
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    SELECT
        *,
        QUARTER(created_at) AS created_at_quarter,
        COALESCE(value, 'OTHER') AS resolved_value
    FROM kept_rows
    WHERE HOUR(created_at) BETWEEN 7 AND 12
),
valid AS (
    SELECT
        id,
        created_at,
        value,
        resolved_value,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        LENGTH(value) AS value_length,
        COALESCE(value, 'DIRECT') AS resolved_value_2
    FROM cleaned
    WHERE created_at > TIMESTAMP '2025-03-01 18:00:00'
),
scored AS (
    SELECT
        id,
        created_at,
        value,
        resolved_value,
        days_since_created_at,
        value_length,
        DENSE_RANK() OVER (PARTITION BY value ORDER BY days_since_created_at NULLS FIRST) AS days_since_created_at_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY value ORDER BY days_since_created_at DESC NULLS LAST) AS days_since_created_at_percent_rank,
        MAX(days_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS days_since_created_at_max_1h
    FROM valid
)
SELECT
    created_at,
    resolved_value,
    days_since_created_at,
    value_length,
    days_since_created_at_dense_rank
FROM scored
