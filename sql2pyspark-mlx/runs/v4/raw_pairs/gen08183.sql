WITH enriched AS (
    SELECT s.*, b.store_name
    FROM store_sales s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
base AS (
    SELECT *, revenue * units AS revenue_per_unit
    FROM enriched
    WHERE YEAR(sale_date) >= 2025
),
with_dims AS (
    SELECT
        src.*,
        d.brand,
        d.price
    FROM base src
    INNER JOIN products d
        ON src.product_id = d.id
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        store_name,
        revenue_per_unit,
        brand,
        price,
        SUM(units) OVER w_recent AS product_id_sum_units,
        ROW_NUMBER() OVER w_recent_2 AS row_seq
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY product_id, store_id), w_recent_2 AS (PARTITION BY store_id ORDER BY revenue_per_unit DESC, id)
)
SELECT
    s.id,
    s.store_id,
    s.units,
    s.store_name,
    s.revenue_per_unit,
    s.brand,
    s.price,
    s.row_seq,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS avg_store_sale_revenue
FROM with_windows s
ORDER BY id DESC
LIMIT 10;
