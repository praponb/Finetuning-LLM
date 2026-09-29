WITH kept AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date < s.launch_date
        )
),
joined AS (
    SELECT
        src.*,
        b.city,
        b.country,
        b.rating
    FROM kept src
    INNER JOIN suppliers b
        ON src.supplier_id = b.id
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY price DESC) AS price_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS event_seq,
        SUM(cost) OVER (PARTITION BY city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
    FROM joined
),
enriched AS (
    SELECT
        id,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        country,
        cumulative_cost,
        MIN(price_dense_rank) OVER (PARTITION BY category, city) AS category_min_price_dense_rank,
        SUM(rating) OVER (ORDER BY cumulative_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        NTILE(10) OVER w_running AS price_dense_rank_bucket
    FROM with_windows
    WHERE cumulative_cost < 1569
    WINDOW w_running AS (PARTITION BY country ORDER BY price_dense_rank, id)
)
SELECT *
FROM (
    SELECT
        brand,
        category,
        country,
        category_min_price_dense_rank
    FROM enriched
) src
PIVOT (
    MAX(category_min_price_dense_rank)
    FOR country IN ('SG' AS sg, 'DE' AS de, 'TH' AS th)
);
