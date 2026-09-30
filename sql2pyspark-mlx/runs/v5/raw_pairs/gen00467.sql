WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, value
),
valid AS (
    SELECT
        *,
        SUBSTRING(k, 1, 3) AS k_prefix,
        COALESCE(k, 'N/A') AS k_clean
    FROM exploded
    WHERE k IS NOT DISTINCT FROM 'utm_source'
),
remaining_keys AS (
    SELECT id, customer_id
    FROM valid
    EXCEPT ALL
    SELECT id, customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 10 AND 14
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
cleaned AS (
    SELECT
        id,
        coupon_codes,
        k,
        value,
        k_prefix,
        k_clean,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS is_spring10,
        COALESCE(k, 'N/A') AS k_filled
    FROM kept_rows
    WHERE customer_id IS NOT NULL
      AND k IS NOT DISTINCT FROM 'utm_source'
),
cleaned_carts AS (
    SELECT
        k,
        value,
        k_prefix,
        k_clean,
        is_spring10,
        ARRAY_JOIN(coupon_codes, ', ') AS coupon_codes_text
    FROM cleaned
    WHERE is_spring10
)
SELECT
    k,
    COUNT_IF(k = 'gift') AS gift_events,
    COUNT_IF(k = 'utm_source') AS utm_source_events,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    SIZE(COLLECT_SET(k)) AS num_ks,
    COUNT(*) FILTER (WHERE k = 'utm_source') AS utm_source_rows,
    COUNT(*) AS num_rows
FROM cleaned_carts
GROUP BY 1
ORDER BY k DESC
