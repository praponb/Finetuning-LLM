WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY brand, category ORDER BY cost DESC, id) AS cost_bucket,
        NTILE(5) OVER (PARTITION BY category ORDER BY cost, id) AS cost_bucket_2
    FROM products
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date >= s.launch_date
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        brand,
        cost,
        tags,
        LEAD(cost_bucket) OVER (PARTITION BY brand ORDER BY price, id) AS next_cost_bucket,
        LAG(cost_bucket) OVER (PARTITION BY brand ORDER BY price, id) AS prev_cost_bucket,
        LAG(price) OVER (PARTITION BY brand ORDER BY price, id) AS prev_price,
        LEAD(price) OVER (PARTITION BY brand ORDER BY price, id) AS next_price,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS row_seq,
        FIRST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY category, supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand
    FROM not_flagged
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY row_seq DESC, id) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        brand,
        next_price,
        prev_price,
        cost
    FROM deduped
) src
UNPIVOT (
    value FOR metric_name IN (next_price, prev_price, cost)
) long_rows;
