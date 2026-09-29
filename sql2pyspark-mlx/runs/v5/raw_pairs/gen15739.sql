WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date >= '2023-12-15'
        )
),
with_last AS (
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.city,
        s.rating,
        latest.product_cost,
        latest.product_launch_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    SELECT
        *,
        AVG(product_cost) OVER (PARTITION BY city, country ORDER BY product_launch_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_cost_avg_3,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY product_cost ASC, id ASC) AS row_seq,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY product_launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM with_last
),
latest AS (
    SELECT
        id,
        supplier_name,
        country,
        rating,
        product_cost,
        product_launch_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY product_launch_date DESC) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
),
stats AS (
    SELECT
        country,
        SUM(CASE WHEN country = 'TH' THEN product_cost ELSE 0 END) AS th_product_cost,
        COUNT(*) AS row_count,
        COUNT(*) AS event_count
    FROM latest
    GROUP BY 1
)
SELECT
    country,
    th_product_cost,
    row_count
FROM stats
ORDER BY th_product_cost ASC, country ASC
