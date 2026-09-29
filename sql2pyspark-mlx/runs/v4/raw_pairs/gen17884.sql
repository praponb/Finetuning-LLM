WITH base AS (
    SELECT *, CASE WHEN price >= 970 THEN 'High' WHEN price >= 460 THEN 'Medium' ELSE 'Low' END AS price_level
    FROM products
    WHERE supplier_id = 33
),
filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        supplier_id,
        price_level,
        LEAST(stock_qty, price) AS least_stock_qty_price
    FROM base
    WHERE launch_date > DATE '2023-08-05'
      AND brand IS NOT NULL
),
latest AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY launch_date, id) = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC NULLS FIRST, id) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
latest_match AS (
    SELECT s.*, x.last_restock
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_products AS (
    SELECT *, RANK() OVER (PARTITION BY supplier_id, category ORDER BY launch_date DESC NULLS FIRST) AS rank_in_group
    FROM latest_match
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        *,
        AVG(least_stock_qty_price) OVER (ORDER BY price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS least_stock_qty_price_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY price NULLS FIRST) AS price_dense_rank,
        ROW_NUMBER() OVER (ORDER BY price DESC, id) AS row_seq,
        NTILE(5) OVER w_grp AS least_stock_qty_price_bucket,
        DENSE_RANK() OVER w_hist AS least_stock_qty_price_dense_rank
    FROM latest_products
    WINDOW w_grp AS (ORDER BY least_stock_qty_price DESC, id), w_hist AS (ORDER BY least_stock_qty_price DESC)
)
SELECT *
FROM (
    SELECT
        supplier_id,
        brand,
        least_stock_qty_price_dense_rank
    FROM metrics
) src
PIVOT (
    AVG(least_stock_qty_price_dense_rank) AS value_sum
    FOR brand IN ('Umbrella' AS umbrella, 'Stark' AS stark)
);
