WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS elem
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        qty,
        price,
        value
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS value
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, d.segment
    FROM flattened_carts s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        qty,
        price,
        segment,
        LENGTH(segment) segment_length,
        COALESCE(value, 'OTHER') resolved_value
    FROM joined
    WHERE value IS NOT NULL
      AND value IN ('BUNDLE', 'VIP', 'SPRING10')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        qty,
        price,
        segment,
        segment_length,
        resolved_value,
        TRIM(segment) clean_segment
    FROM cleaned
    WHERE customer_id IN (3, 30)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(segment_length) OVER (PARTITION BY customer_id) customer_id_sum_segment_length,
        RANK() OVER (PARTITION BY segment ORDER BY qty DESC NULLS LAST) qty_rank,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY qty DESC NULLS LAST) qty_cume_dist
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(segment) segment_lower
    FROM enriched
    WHERE customer_id_sum_segment_length >= 153
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    MAX(segment_length) FILTER (WHERE segment = 'SMB') AS smb_max_segment_length,
    COUNT(DISTINCT segment) AS distinct_segments,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    SIZE(COLLECT_SET(segment)) AS num_segments
FROM valid
GROUP BY customer_id
ORDER BY corporate_count DESC
