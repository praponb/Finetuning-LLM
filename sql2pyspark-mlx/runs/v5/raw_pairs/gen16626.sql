WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v
),
matched_keys AS (
    SELECT id
    FROM flattened
    EXCEPT
    SELECT id
    FROM carts
    WHERE created_at >= '2023-09-01'
),
reconciled AS (
    SELECT s.*
    FROM flattened s
    JOIN matched_keys k ON s.id = k.id
),
base AS (
    SELECT
        id,
        coupon_codes,
        k,
        v,
        LOWER(k) k_lower
    FROM reconciled
    WHERE created_at >= TO_DATE('2025-03-01')
      AND k IS DISTINCT FROM 'utm_source'
),
cleaned AS (
    SELECT
        *,
        ARRAY_JOIN(coupon_codes, ', ') coupon_codes_text,
        ARRAY_JOIN(coupon_codes, '|') coupon_codes_text_2,
        COALESCE(k, 'N/A') k_filled
    FROM base
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
remaining_keys AS (
    SELECT id
    FROM cleaned
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE customer_id IN (10, 14, 20)
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id
)
SELECT
    k,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids_3
FROM verified
GROUP BY k;
