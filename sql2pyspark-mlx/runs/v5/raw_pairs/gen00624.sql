WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CEIL(quantity) AS quantity_whole,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE last_restock >= DATE '2023-08-01'
      AND store_id IN (2, 6)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY quantity_whole) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.product_name,
        d.brand,
        d.category
    FROM deduped s
    LEFT JOIN products d
        ON s.product_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.last_restock_month_end,
    s.dedup_rank,
    s.product_name,
    s.brand,
    (SELECT AVG(s2.revenue) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS avg_store_sale_revenue
FROM latest s
ORDER BY product_id DESC NULLS LAST;
