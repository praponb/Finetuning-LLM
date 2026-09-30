WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        price,
        launch_date,
        supplier_id,
        CASE WHEN price >= 1220 THEN 'High' WHEN price >= 230 THEN 'Medium' ELSE 'Low' END AS price_band,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
      AND stock_qty <= 314
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock <= s.launch_date
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(price, 1) OVER (PARTITION BY category, supplier_id ORDER BY price, id) AS next_price,
        LEAD(price, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_price_2,
        COUNT(price) OVER (PARTITION BY supplier_id) AS supplier_id_count_price
    FROM kept
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id) AS dedup_rank
        FROM ranked
    ) t
    WHERE dedup_rank = 1
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.store_sale_revenue,
        x.units
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.launch_date,
    s.supplier_id,
    s.price_band,
    s.brand_clean,
    s.next_price,
    s.next_price_2,
    (SELECT MIN(x.cost) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_cost
FROM latest_match s
ORDER BY category
