WITH enriched AS (
    SELECT e.*, p.country
    FROM products e
    INNER JOIN suppliers p
        ON e.supplier_id = p.id
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY cost DESC, id) AS row_seq,
        MAX(price) OVER w_recent AS brand_max_price
    FROM enriched
    WINDOW w_recent AS (PARTITION BY brand, category)
),
not_flagged AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id IN (33, 36)
        )
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY price DESC) AS price_dense_rank,
        AVG(row_seq) OVER w_hist AS row_seq_avg_3_prior,
        SUM(cost) OVER w_recent AS cumulative_cost,
        AVG(stock_qty) OVER (PARTITION BY supplier_id, country ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS stock_qty_avg_6
    FROM not_flagged
    WINDOW w_hist AS (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_recent AS (PARTITION BY country, supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_products AS (
    SELECT
        src.*,
        p.supplier_name,
        p.country AS supplier_country
    FROM metrics src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
)
SELECT *
FROM (
    SELECT
        supplier_id,
        supplier_country,
        cumulative_cost
    FROM enriched_products
) src
PIVOT (
    SUM(cumulative_cost) AS total
    FOR supplier_country IN ('US', 'TH', 'DE')
);
