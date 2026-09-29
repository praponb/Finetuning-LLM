WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        product_id,
        quantity
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST) AS quantity_rank,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS row_seq
    FROM ranked
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        AVG(quantity_rank) OVER (PARTITION BY product_id ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_rank_avg_2_prior,
        AVG(row_seq) OVER (PARTITION BY product_id ORDER BY quantity_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY row_seq DESC, id) AS event_seq,
        AVG(row_seq) OVER (PARTITION BY product_id ORDER BY quantity_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_seq_avg_6
    FROM metrics
    WHERE quantity_rank < 5
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM inventory
    WHERE YEAR(last_restock) >= 2024
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.category,
        b.cost,
        b.product_name
    FROM verified a
    LEFT JOIN products b
        ON a.product_id = b.id
),
joined_inventory AS (
    -- Attach products attributes
    SELECT
        s.product_id,
        s.row_seq_avg_6_prior,
        s.row_seq_avg_6,
        s.product_name,
        b.category AS product_category,
        b.launch_date
    FROM joined s
    INNER JOIN products b
        ON s.product_id = b.id
)
SELECT
    s.product_id,
    s.row_seq_avg_6_prior,
    s.product_category,
    s.launch_date,
    (SELECT MAX(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS max_store_sale_units
FROM joined_inventory s
ORDER BY product_category
