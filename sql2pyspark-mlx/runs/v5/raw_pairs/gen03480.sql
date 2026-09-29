WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label_2
    FROM inventory
    WHERE last_restock < '2025-03-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        quantity,
        last_restock_label_2,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS quantity_rank,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist
    FROM cleaned
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.store_name,
        d.opened_date,
        d.sqft
    FROM scored s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
scored_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity_avg_2,
        opened_date,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_rank, id) AS seq_no,
        SUM(sqft) OVER (PARTITION BY store_id ORDER BY quantity_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM with_dims
),
scored_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        sqft,
        seq_no,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        RANK() OVER w_part AS quantity_avg_2_rank,
        NTILE(3) OVER (ORDER BY seq_no, id) AS seq_no_bucket
    FROM scored_inventory
    WINDOW w_part AS (ORDER BY quantity_avg_2 DESC NULLS FIRST)
)
SELECT
    id,
    sqft,
    seq_no,
    running_sqft,
    quantity_avg_2_rank,
    seq_no_bucket
FROM scored_inventory_2;
