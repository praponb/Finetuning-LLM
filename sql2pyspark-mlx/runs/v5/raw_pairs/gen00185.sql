WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        CUME_DIST() OVER w_part AS quantity_cume_dist,
        LEAD(quantity, 1) OVER w_ordered AS next_quantity
    FROM inventory
    WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST), w_ordered AS (PARTITION BY store_id ORDER BY quantity ASC, id ASC)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM enriched
    WHERE next_quantity < 77
),
filtered_inventory AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last_restock,
        quantity_cume_dist,
        last_restock_month_end,
        DATE_ADD(last_restock, 53) AS last_restock_plus_53d,
        CAST(next_quantity AS DOUBLE) AS next_quantity_str
    FROM filtered
    WHERE last_restock > DATE '2023-07-12'
      AND last_restock_month_end BETWEEN '2024-12-01' AND '2025-10-05'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        last_restock_plus_53d,
        NTILE(10) OVER w_recent AS next_quantity_str_bucket,
        SUM(next_quantity_str) OVER w_ordered AS cumulative_next_quantity_str
    FROM filtered_inventory
    WINDOW w_recent AS (ORDER BY next_quantity_str DESC, id ASC), w_ordered AS (ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        last_restock_plus_53d,
        next_quantity_str_bucket,
        cumulative_next_quantity_str,
        NTILE(4) OVER (ORDER BY next_quantity_str_bucket ASC, id ASC) AS next_quantity_str_bucket_bucket,
        DENSE_RANK() OVER (ORDER BY next_quantity_str_bucket ASC NULLS FIRST) AS next_quantity_str_bucket_dense_rank,
        CUME_DIST() OVER (ORDER BY cumulative_next_quantity_str ASC) AS cumulative_next_quantity_str_cume_dist,
        FIRST_VALUE(cumulative_next_quantity_str, TRUE) OVER (ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_next_quantity_str,
        ROW_NUMBER() OVER (ORDER BY last_restock DESC, id ASC) AS seq_no
    FROM metrics
    WHERE last_restock_plus_53d BETWEEN '2023-06-01' AND '2025-05-05'
)
SELECT
    id,
    last_restock,
    last_restock_plus_53d,
    next_quantity_str_bucket,
    next_quantity_str_bucket_bucket,
    cumulative_next_quantity_str_cume_dist
FROM scored
ORDER BY cumulative_next_quantity_str_cume_dist ASC
