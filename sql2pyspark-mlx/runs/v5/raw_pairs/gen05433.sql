WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default,
        SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines,
        SIZE(items) AS num_items
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    coupon_codes,
    NTILE(3) OVER (PARTITION BY customer_id ORDER BY bulk_lines, id) AS bulk_lines_bucket,
    LEAD(bulk_lines, 1) OVER (PARTITION BY customer_id ORDER BY bulk_lines, id) AS next_bulk_lines
FROM parsed;
