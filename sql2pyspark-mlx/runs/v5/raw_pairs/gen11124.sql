WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS(',', country, city) AS country_city_key
    FROM suppliers
    WHERE city IS NOT DISTINCT FROM 'Chiang Mai'
      AND rating > 2.6
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_cost,
        top1.product_launch_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_launch_date, id) AS event_seq,
        COUNT(product_cost) OVER (PARTITION BY country) AS country_count_product_cost,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM latest_match
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        rating,
        country_city_key,
        product_launch_date,
        cumulative_rating,
        LEAD(cumulative_rating) OVER w_grp AS next_cumulative_rating,
        LAG(cumulative_rating) OVER w_grp AS prev_cumulative_rating,
        LEAD(rating) OVER w_grp AS next_rating,
        ROW_NUMBER() OVER w_grp AS event_seq_2
    FROM scored
    WINDOW w_grp AS (PARTITION BY city, country ORDER BY product_cost, id)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date < s.product_launch_date
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_stock_qty
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    supplier_name,
    product_launch_date,
    next_cumulative_rating,
    prev_cumulative_rating,
    next_rating,
    event_seq_2,
    product_stock_qty
FROM with_last;
