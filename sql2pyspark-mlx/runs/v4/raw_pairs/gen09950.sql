WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(last_restock, 51) AS last_restock_plus_51d
    FROM inventory
    WHERE last_restock >= DATE '2023-07-06'
      AND last_restock BETWEEN '2023-08-24' AND '2025-07-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_plus_51d,
        NTILE(4) OVER (ORDER BY quantity DESC, id) AS quantity_bucket,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock_plus_51d, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_prior
    FROM base
),
scored_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        NTILE(10) OVER w_ordered AS quantity_bucket_bucket,
        SUM(quantity_bucket) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_bucket
    FROM scored
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY quantity_bucket DESC, id)
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, p.price
    FROM scored_inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.last_restock_plus_51d,
    s.quantity_bucket,
    s.quantity_avg_3_prior,
    s.quantity_bucket_bucket,
    (SELECT MAX(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS max_store_sale_units
FROM with_dims s
ORDER BY last_restock_plus_51d DESC;
