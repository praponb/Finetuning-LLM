WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_num
    FROM inventory
    WHERE last_restock >= DATE '2023-03-16'
      AND product_id IN (1, 4, 7, 8)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        quantity_num,
        COUNT(quantity_num) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity_num,
        AVG(quantity_num) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity_num,
        MAX(quantity_num) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity_num,
        RANK() OVER (ORDER BY quantity DESC) AS quantity_rank
    FROM cleaned
),
cleaned_inventory AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(running_max_quantity_num, 0) AS running_max_quantity_num_rounded
    FROM metrics
    WHERE last_restock >= DATE '2023-06-23'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        quantity_num,
        running_count_quantity_num,
        running_avg_quantity_num,
        running_max_quantity_num_rounded,
        SUM(quantity_num) OVER w_recent AS running_quantity_num,
        FIRST_VALUE(quantity_rank) IGNORE NULLS OVER w_recent AS first_known_quantity_rank,
        RANK() OVER (ORDER BY quantity_num DESC NULLS FIRST) AS quantity_num_rank
    FROM cleaned_inventory
    WHERE running_avg_quantity_num IS NOT NULL
    WINDOW w_recent AS (ORDER BY quantity_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    quantity_num,
    running_count_quantity_num,
    running_avg_quantity_num,
    running_max_quantity_num_rounded,
    running_quantity_num,
    first_known_quantity_rank
FROM enriched
ORDER BY first_known_quantity_rank DESC NULLS LAST;
