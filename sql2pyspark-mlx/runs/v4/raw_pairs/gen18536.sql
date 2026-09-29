WITH enriched AS (
    -- Attach suppliers attributes
    SELECT a.*, d.country
    FROM products a
    LEFT JOIN suppliers d
        ON a.supplier_id = d.id
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.country AS supplier_country,
        d.rating
    FROM enriched e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.unit_price >= 150
        )
)
-- Window metrics over several different partitions
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    country,
    supplier_country,
    rating,
    AVG(stock_qty) OVER (ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS stock_qty_avg_3,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
    ROW_NUMBER() OVER (PARTITION BY supplier_country ORDER BY price, id) AS seq_no,
    CORR(stock_qty, cost) OVER (PARTITION BY brand) AS corr_stock_qty_cost,
    CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_cume_dist
FROM not_flagged;
