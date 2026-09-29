WITH latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.stock_qty,
        x.quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS stock_qty_bucket,
        COUNT(stock_qty) OVER (PARTITION BY category) AS category_count_stock_qty,
        LEAD(quantity, 2, 0) OVER (ORDER BY quantity, id) AS next_quantity,
        AVG(stock_qty) OVER (ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_5_prior
    FROM latest_match
    WHERE stock_qty >= 263
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.stock_qty,
        s.quantity,
        s.stock_qty_bucket,
        s.stock_qty_avg_5_prior,
        x.unit_price
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        stock_qty_bucket,
        stock_qty_avg_5_prior,
        unit_price,
        stock_qty_avg_5_prior / NULLIF(quantity, 0) AS stock_qty_avg_5_prior_per_quantity
    FROM with_last
    WHERE stock_qty_avg_5_prior = 172
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY stock_qty_avg_5_prior_per_quantity) AS stock_qty_avg_5_prior_per_quantity_percent_rank,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS seq_no
    FROM cleaned
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock BETWEEN '2023-03-01' AND '2024-06-05'
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        stock_qty_bucket,
        seq_no,
        LAG(seq_no, 1) OVER w_ordered AS prev_seq_no,
        ROW_NUMBER() OVER w_ordered AS seq_no_2
    FROM kept
    WINDOW w_ordered AS (ORDER BY stock_qty_avg_5_prior_per_quantity_percent_rank, id)
)
-- Grouped metrics
SELECT
    seq_no,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    PERCENTILE_APPROX(seq_no_2, ARRAY(0.25, 0.5, 0.75)) AS seq_no_2_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(prev_seq_no) AS avg_prev_seq_no,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM with_windows
GROUP BY 1
HAVING distinct_ids >= 1
ORDER BY seq_no NULLS LAST;
