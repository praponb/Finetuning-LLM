WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date, id) AS seq_no,
        AVG(cost) OVER (PARTITION BY supplier_id, category ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        AVG(stock_qty) OVER (PARTITION BY category) AS category_avg_stock_qty
    FROM products
),
enriched_products AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS seq_no_2,
        FIRST_VALUE(seq_no, TRUE) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3,
        RANK() OVER (PARTITION BY brand ORDER BY cost_avg_6 DESC NULLS FIRST) AS cost_avg_6_rank
    FROM enriched
    WINDOW w_hist AS (ORDER BY category_avg_stock_qty, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.store_id = 2
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        brand,
        tags,
        supplier_id,
        seq_no,
        cost_avg_6,
        cost_avg_6_rank,
        FIRST_VALUE(cost_avg_3) IGNORE NULLS OVER (PARTITION BY brand ORDER BY cost_avg_6_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_avg_3,
        ROW_NUMBER() OVER (ORDER BY cost DESC, id) AS row_seq
    FROM not_flagged
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.rating,
        d.country,
        d.city
    FROM metrics e
    LEFT JOIN suppliers d
        ON e.supplier_id <=> d.id
    WHERE d.rating BETWEEN 2.4 AND 3.5
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        first_known_cost_avg_3,
        row_seq,
        rating,
        city,
        AVG(row_seq) OVER (PARTITION BY category ORDER BY first_known_cost_avg_3, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_seq_avg_2,
        SUM(seq_no) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        LAG(cost_avg_6, 1) OVER (PARTITION BY country ORDER BY seq_no, id) AS prev_cost_avg_6,
        COUNT(cost_avg_6) OVER (PARTITION BY supplier_id) AS supplier_id_count_cost_avg_6,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost_avg_6_rank, id) AS event_seq
    FROM enriched_products_2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        rating,
        city,
        running_seq_no,
        event_seq,
        LEAD(rating) OVER w_hist AS next_rating,
        LAG(rating) OVER w_hist AS prev_rating,
        LAG(row_seq) OVER w_hist AS prev_row_seq,
        ROW_NUMBER() OVER w_hist AS row_seq_2,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, category), 4) AS rating_share
    FROM scored
    WINDOW w_hist AS (PARTITION BY city ORDER BY supplier_id_count_cost_avg_6, id)
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            category,
            city,
            row_seq_2
        FROM with_windows
    ) src
    PIVOT (
        AVG(row_seq_2) AS amount,
        COUNT(1) AS n
        FOR city IN ('New York' AS new_york, 'Singapore' AS singapore)
    )
)
SELECT
    category,
    new_york_n,
    singapore_amount,
    singapore_n
FROM pivoted
ORDER BY singapore_n NULLS LAST, singapore_amount DESC
