WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        ship_to.zip AS ship_zip,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default
    FROM carts
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min_2,
    GROUPING(DATE_TRUNC('HOUR', created_at)) AS grouping_level
FROM flattened_attrs
GROUP BY ROLLUP (DATE_TRUNC('HOUR', created_at));
