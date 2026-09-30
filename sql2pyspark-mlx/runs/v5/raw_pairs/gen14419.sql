WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        coupon_codes,
        ship_to,
        attributes,
        SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown
    FROM carts
    WHERE SIZE(coupon_codes) > 1
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    PERCENTILE_APPROX(bulk_lines, 0.95) AS bulk_lines_p95,
    APPROX_PERCENTILE(bulk_lines, ARRAY(0.25, 0.5, 0.75)) AS bulk_lines_quartiles,
    ROUND(AVG(bulk_lines), 2) AS avg_bulk_lines,
    APPROX_PERCENTILE(bulk_lines, ARRAY(0.25, 0.5, 0.75)) AS bulk_lines_quartiles_2,
    SUM(bulk_lines) AS total_bulk_lines,
    SUM(bulk_lines) AS total_bulk_lines_2
FROM parsed
GROUP BY 1
ORDER BY total_bulk_lines ASC NULLS LAST;
