WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(cost) AS cost_whole,
        CONCAT(product_name, ' - ', brand) AS product_name_brand
    FROM products
    WHERE launch_date > DATE '2023-04-23'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.revenue BETWEEN 1400 AND 3100
        )
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.country,
        b.rating
    FROM kept e
    INNER JOIN suppliers b
        ON e.supplier_id = b.id
    WHERE b.rating BETWEEN 1.9 AND 4.0
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(price) OVER w_part AS next_price,
        LEAD(rating) OVER w_part AS next_rating,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM joined
    WINDOW w_part AS (PARTITION BY brand ORDER BY stock_qty, id)
)
-- Join a dimension and aggregate
SELECT
    d.city,
    src.brand,
    COUNT(*) AS num_rows,
    ROUND(AVG(src.next_rating), 2) AS avg_next_rating,
    MAX(src.seq_no) AS max_seq_no,
    GROUPING_ID() AS gid
FROM enriched src
INNER JOIN suppliers d ON src.supplier_id = d.id
GROUP BY GROUPING SETS ((d.city, src.brand), (d.city), ());
