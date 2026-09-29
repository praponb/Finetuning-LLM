WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(status) AS status_length,
        CONCAT_WS('-', carrier, status) AS carrier_status_key,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE shipped_date >= DATE '2023-08-07'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(status_length) IGNORE NULLS OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_length,
        LAG(status_length, 1) OVER (PARTITION BY status, carrier ORDER BY cost, id) AS prev_status_length,
        SUM(cost) OVER w_grp AS cumulative_cost,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY status_length NULLS LAST) AS status_length_dense_rank,
        SUM(status_length) OVER (PARTITION BY order_id, status) AS order_id_sum_status_length
    FROM base
    WINDOW w_grp AS (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        status_length,
        carrier_status_key,
        carrier_clean,
        first_known_status_length,
        cumulative_cost,
        status_length_dense_rank,
        order_id_sum_status_length,
        order_id_sum_status_length * prev_status_length AS order_id_sum_status_length_times_prev_status_length,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM metrics
    WHERE shipped_date >= '2025-07-19'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        shipped_date,
        cost,
        status,
        status_length,
        carrier_status_key,
        cumulative_cost,
        status_length_dense_rank,
        order_id_sum_status_length,
        order_id_sum_status_length_times_prev_status_length,
        carrier_filled
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_length_dense_rank, id) = 1
)
-- Grouped metrics
SELECT
    status,
    CASE WHEN status_length_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS status_length_dense_rank_tier,
    ROUND(AVG(cost), 2) AS avg_cost,
    MIN(status_length) AS min_status_length
FROM deduped
GROUP BY GROUPING SETS ((status, CASE WHEN status_length_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END), (status))
ORDER BY status;
