WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.city,
        s.country,
        s.supplier_name
    FROM products a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        supplier_name,
        LAG(stock_qty) OVER w_recent AS prev_stock_qty,
        LEAD(stock_qty) OVER w_recent AS next_stock_qty,
        LAG(price) OVER w_recent AS prev_price,
        LEAD(price) OVER w_recent AS next_price,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM joined
    WHERE country = 'DE'
    WINDOW w_recent AS (PARTITION BY supplier_id ORDER BY price, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        prev_stock_qty,
        seq_no,
        LAST_VALUE(cost) IGNORE NULLS OVER w_grp AS last_known_cost,
        LEAD(stock_qty, 2, 0) OVER (PARTITION BY category, brand ORDER BY next_stock_qty, id) AS next_stock_qty_2
    FROM scored
    WINDOW w_grp AS (PARTITION BY category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT ALL
    SELECT id
    FROM products
    WHERE cost < 570
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        last_known_cost,
        next_stock_qty_2,
        PERCENT_RANK() OVER w_hist AS prev_stock_qty_percent_rank,
        RANK() OVER w_hist_2 AS last_known_cost_rank,
        PERCENT_RANK() OVER w_grp AS price_percent_rank
    FROM kept_rows
    WINDOW w_hist AS (ORDER BY prev_stock_qty DESC NULLS FIRST), w_hist_2 AS (ORDER BY last_known_cost NULLS FIRST), w_grp AS (ORDER BY price NULLS LAST)
)
-- Window metrics over several different partitions
SELECT
    next_stock_qty_2,
    prev_stock_qty_percent_rank,
    last_known_cost_rank,
    DENSE_RANK() OVER (ORDER BY next_stock_qty_2 DESC) AS next_stock_qty_2_dense_rank,
    DENSE_RANK() OVER w_part AS next_stock_qty_2_dense_rank_2,
    RANK() OVER (ORDER BY last_known_cost DESC NULLS LAST) AS last_known_cost_rank_2,
    DENSE_RANK() OVER w_running AS last_known_cost_dense_rank,
    CUME_DIST() OVER w_ordered AS price_percent_rank_cume_dist
FROM metrics
WINDOW w_part AS (ORDER BY next_stock_qty_2 DESC NULLS FIRST), w_running AS (ORDER BY last_known_cost), w_ordered AS (ORDER BY price_percent_rank NULLS FIRST);
