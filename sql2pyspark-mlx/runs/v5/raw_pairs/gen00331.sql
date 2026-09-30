WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        YEAR(launch_date) AS launch_date_year
    FROM products
    WHERE brand IS NOT NULL
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_sale_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(stock_qty) OVER w_grp AS running_max_stock_qty,
        MIN(stock_qty) OVER w_grp AS running_min_stock_qty,
        SUM(stock_qty) OVER w_grp AS running_sum_stock_qty,
        SUM(launch_date_year) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_launch_date_year
    FROM with_last
    WHERE cost >= 400
    WINDOW w_grp AS (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT category, brand
    FROM with_windows
    INTERSECT
    SELECT category, brand
    FROM products
    WHERE supplier_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.category = k.category AND s.brand = k.brand
)
-- Ranking and gap features
SELECT
    brand,
    launch_date,
    launch_date_year,
    running_min_stock_qty,
    running_sum_stock_qty,
    CUME_DIST() OVER (ORDER BY running_launch_date_year DESC) AS running_launch_date_year_cume_dist,
    PERCENT_RANK() OVER (ORDER BY running_sum_stock_qty NULLS FIRST) AS running_sum_stock_qty_percent_rank
FROM kept_rows
