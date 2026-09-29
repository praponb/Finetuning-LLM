WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY category, brand ORDER BY price DESC NULLS LAST) AS dedup_rank
    FROM products
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        dedup_rank,
        LAG(price, 2) OVER (PARTITION BY supplier_id ORDER BY dedup_rank, id) prev_price,
        LAST_VALUE(stock_qty, TRUE) OVER w_running last_known_stock_qty,
        ROW_NUMBER() OVER w_part seq_no,
        NTILE(3) OVER (PARTITION BY brand ORDER BY cost DESC, id) cost_bucket,
        LEAD(stock_qty, 2) OVER (PARTITION BY supplier_id ORDER BY price, id) next_stock_qty
    FROM latest
    WINDOW w_running AS (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY category, supplier_id ORDER BY price, id)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT
    SELECT id
    FROM products
    WHERE SIZE(tags) >= 1
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.country,
        d.city,
        d.rating
    FROM reconciled e
    INNER JOIN suppliers d
        ON e.supplier_id = d.id
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        b.rating AS supplier_rating,
        b.city AS supplier_city
    FROM enriched_products a
    LEFT JOIN suppliers b
        ON a.supplier_id = b.id
),
enriched_products_3 AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.country AS supplier_country,
        s.city AS supplier_city_2,
        s.rating AS supplier_rating_2
    FROM enriched_products_2 src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(cost, price) OVER (PARTITION BY supplier_id) corr_cost_price,
        CUME_DIST() OVER (ORDER BY supplier_rating_2) supplier_rating_2_cume_dist,
        PERCENT_RANK() OVER (ORDER BY stock_qty NULLS FIRST) stock_qty_percent_rank
    FROM enriched_products_3
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        supplier_city_2,
        stock_qty_percent_rank,
        supplier_rating_2_cume_dist,
        supplier_rating,
        supplier_rating_2
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (stock_qty_percent_rank, supplier_rating_2_cume_dist, supplier_rating, supplier_rating_2)
);
