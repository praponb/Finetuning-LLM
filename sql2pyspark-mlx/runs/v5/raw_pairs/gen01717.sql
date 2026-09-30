WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(price / cost, 2) AS margin
    FROM products
    WHERE product_name LIKE 'Wa%'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        margin,
        ROW_NUMBER() OVER w_hist AS event_seq,
        SUM(price) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price
    FROM cleaned
    WINDOW w_hist AS (ORDER BY cost DESC, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC NULLS FIRST, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock >= '2025-07-01'
        )
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.price,
        s.launch_date,
        s.cumulative_price,
        s.row_num,
        top1.store_sale_revenue
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        NTILE(3) OVER (ORDER BY price, id) AS price_bucket,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM with_last
    WINDOW w_ordered AS (ORDER BY launch_date, id)
)
-- Running and moving aggregates
SELECT
    id,
    price,
    launch_date,
    SUM(price) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price_2,
    SUM(seq_no) OVER (ORDER BY price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
    ROW_NUMBER() OVER (ORDER BY row_seq DESC, id) AS seq_no_2,
    PERCENT_RANK() OVER (ORDER BY price NULLS FIRST) AS price_percent_rank
FROM metrics
