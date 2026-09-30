WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
    FROM inventory
    WHERE product_id IN (6, 8, 9)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_months DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_plus_months DESC, id) AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_months DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY last_restock_plus_months, id) AS seq_no_3,
        ROW_NUMBER() OVER (ORDER BY last_restock_plus_months, id) AS seq_no_4
    FROM valid
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY seq_no, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.product_name,
        b.launch_date
    FROM deduped src
    LEFT JOIN products b
        ON src.product_id = b.id
)
SELECT
    s.id,
    s.product_id,
    s.last_restock_plus_months,
    s.seq_no,
    s.row_seq,
    s.seq_no_3,
    s.seq_no_4,
    s.dedup_rank,
    s.product_name,
    (SELECT MAX(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS max_inventory_quantity
FROM joined s
ORDER BY seq_no_4 DESC NULLS FIRST;
