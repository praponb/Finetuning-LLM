WITH enriched AS (
    SELECT
        e.id,
        e.product_name,
        e.category,
        e.brand,
        e.cost,
        e.stock_qty,
        e.supplier_id,
        d.country,
        d.city
    FROM products e
    LEFT JOIN suppliers d
        ON e.supplier_id <=> d.id
    WHERE d.country NOT IN ('SG', 'TH', 'US')
),
with_windows AS (
    SELECT
        id,
        category,
        cost,
        stock_qty,
        supplier_id,
        country,
        LAG(stock_qty) OVER w_running AS prev_stock_qty,
        LEAD(stock_qty) OVER w_running AS next_stock_qty,
        LAG(cost) OVER w_running AS prev_cost,
        LEAD(cost) OVER w_running AS next_cost,
        ROW_NUMBER() OVER w_running AS row_seq,
        AVG(cost) OVER w_running_2 AS country_avg_cost
    FROM enriched
    WINDOW w_running AS (PARTITION BY category, brand ORDER BY stock_qty, id), w_running_2 AS (PARTITION BY country)
),
valid AS (
    SELECT
        id,
        category,
        stock_qty,
        supplier_id,
        country,
        prev_stock_qty,
        next_stock_qty,
        prev_cost,
        next_cost,
        row_seq,
        country_avg_cost,
        CEIL(row_seq) AS row_seq_whole
    FROM with_windows
    WHERE cost >= 220
),
with_latest AS (
    SELECT s.*, top1.inventory_last_restock
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, country ORDER BY inventory_last_restock DESC, id) AS dedup_rank
    FROM with_latest
    QUALIFY dedup_rank = 1
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY country, category ORDER BY prev_stock_qty DESC NULLS FIRST) AS prev_stock_qty_dense_rank,
        SUM(country_avg_cost) OVER (ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_country_avg_cost,
        LAST_VALUE(row_seq_whole) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY row_seq_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq_whole,
        ROUND(next_cost / SUM(next_cost) OVER (PARTITION BY supplier_id), 4) AS next_cost_share,
        AVG(stock_qty) OVER (PARTITION BY supplier_id ORDER BY next_stock_qty, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_5_prior
    FROM ranked
),
daily AS (
    SELECT
        CASE WHEN stock_qty >= 424 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
        BIT_AND(stock_qty) AS stock_qty_bit_and,
        PERCENTILE_APPROX(cumulative_country_avg_cost, ARRAY(0.25, 0.5, 0.75)) AS cumulative_country_avg_cost_quartiles
    FROM scored
    GROUP BY stock_qty_tier
    HAVING stock_qty_bit_and > 1
)
SELECT
    stock_qty_tier,
    stock_qty_bit_and,
    cumulative_country_avg_cost_quartiles
FROM daily
ORDER BY stock_qty_tier
LIMIT 20
