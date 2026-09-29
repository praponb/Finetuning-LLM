WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE last_restock > DATE '2023-04-28'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS event_seq,
        COUNT(quantity) OVER (PARTITION BY product_id, store_id) AS product_id_count_quantity,
        COUNT(quantity) OVER (PARTITION BY store_id) AS store_id_count_quantity,
        RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC) AS quantity_rank,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS seq_no
    FROM valid
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_str,
        product_id_count_quantity,
        store_id_count_quantity,
        seq_no,
        CASE WHEN quantity >= 251 THEN 'High' WHEN quantity >= 68 THEN 'Medium' ELSE 'Low' END AS quantity_tier,
        CAST(event_seq AS STRING) AS event_seq_str
    FROM metrics
    WHERE seq_no BETWEEN 5 AND 7
),
metrics_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_str,
        store_id_count_quantity,
        seq_no,
        quantity_tier,
        event_seq_str,
        FIRST_VALUE(store_id_count_quantity, TRUE) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_store_id_count_quantity,
        LAG(product_id_count_quantity, 1) OVER (PARTITION BY product_id ORDER BY seq_no, id) AS prev_product_id_count_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM filtered
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity_str,
        e.store_id_count_quantity,
        e.seq_no,
        e.first_known_store_id_count_quantity,
        e.prev_product_id_count_quantity,
        e.cumulative_quantity,
        b.category,
        b.brand,
        b.product_name
    FROM metrics_inventory e
    LEFT JOIN products b
        ON e.product_id = b.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.brand AS product_brand,
        p.cost
    FROM enriched e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'new')
)
-- Join a dimension and aggregate
SELECT
    dim.brand AS product_brand,
    dim.category AS product_category,
    s.category AS inventory_category,
    COUNT(*) AS cnt,
    ROUND(SUM(s.store_id_count_quantity), 2) AS total_store_id_count_quantity
FROM with_dims s
INNER JOIN products dim ON s.product_id = dim.id
GROUP BY dim.brand, dim.category, s.category
ORDER BY total_store_id_count_quantity NULLS LAST;
