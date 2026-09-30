WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(city) AS city_length,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-08-10'
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_units
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_order_date,
        top1.status
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        city_length,
        resolved_region,
        order_order_date,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY sqft, id) AS dedup_rank
        FROM latest_match
    ) t
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    status,
    DATE_TRUNC('WEEK', order_order_date) AS week_start,
    ROUND(SUM(city_length) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) AS city_length_per_shipped,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(city_length), 2) AS avg_city_length,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count
FROM latest
GROUP BY status, DATE_TRUNC('WEEK', order_order_date)
ORDER BY status DESC NULLS FIRST;
