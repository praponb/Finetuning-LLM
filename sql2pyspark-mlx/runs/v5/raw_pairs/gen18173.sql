WITH with_last AS (
    SELECT
        s.*,
        top1.sale_date,
        top1.store_sale_units
    FROM stores s
    JOIN LATERAL (
            SELECT c.sale_date, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sale_date,
    store_sale_units,
    CASE WHEN city RLIKE '^(Bangkok|Berlin)$' THEN 'CORE' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS city_zone
FROM with_last
ORDER BY store_sale_units DESC
