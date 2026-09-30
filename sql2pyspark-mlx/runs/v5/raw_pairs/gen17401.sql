WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.product_id IN (1, 3, 8)
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        tags,
        supplier_id,
        LEAD(cost, 1, 0) OVER (PARTITION BY brand ORDER BY stock_qty, id) AS next_cost,
        CORR(stock_qty, price) OVER (PARTITION BY brand) AS corr_stock_qty_price,
        AVG(cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior
    FROM not_flagged
    WHERE SIZE(tags) > 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        LAG(cost, 1) OVER (ORDER BY cost_avg_6_prior, id) AS prev_cost,
        SUM(cost_avg_6_prior) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_avg_6_prior,
        LEAD(price, 1) OVER (PARTITION BY supplier_id ORDER BY cost_avg_6_prior, id) AS next_price
    FROM scored
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        d.rating,
        d.country,
        d.city
    FROM with_windows a
    LEFT JOIN suppliers d
        ON a.supplier_id <=> d.id
),
not_flagged_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id IN (6, 9)
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, city ORDER BY cost_bucket, id) AS dedup_rank
    FROM not_flagged_products
    QUALIFY dedup_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        supplier_id,
        prev_cost,
        next_price,
        corr_stock_qty_price
    FROM deduped
) src
UNPIVOT (
    val FOR measure IN (prev_cost, next_price, corr_stock_qty_price)
) long_rows
