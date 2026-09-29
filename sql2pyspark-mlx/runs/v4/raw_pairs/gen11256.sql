WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS DOUBLE) AS quantity_num,
        CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE product_id = 2
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_num,
        quantity_str,
        LEAD(quantity, 2) OVER (ORDER BY quantity, id) AS next_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        SUM(quantity_num) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_num,
        SUM(quantity_num) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_num,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_num DESC, id) AS event_seq
    FROM valid
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_num, id) AS event_seq_2,
        SUM(running_quantity_num) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_quantity_num,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity_num DESC NULLS FIRST) AS quantity_num_percent_rank,
        DENSE_RANK() OVER (ORDER BY quantity NULLS LAST) AS quantity_dense_rank
    FROM metrics
),
daily AS (
    SELECT
        product_id,
        ROUND(AVG(running_quantity), 2) AS avg_running_quantity,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        ROUND(AVG(quantity_num_percent_rank), 2) AS avg_quantity_num_percent_rank,
        ROUND(AVG(running_quantity_num), 2) AS avg_running_quantity_num,
        APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles
    FROM enriched
    GROUP BY product_id
)
SELECT
    product_id,
    avg_running_quantity,
    distinct_store_ids,
    avg_quantity_num_percent_rank,
    event_seq_quartiles
FROM daily
ORDER BY avg_quantity_num_percent_rank DESC
