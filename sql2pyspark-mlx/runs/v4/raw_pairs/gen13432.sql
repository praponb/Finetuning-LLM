WITH base AS (
    SELECT
        id,
        product_id,
        last_restock,
        DATE_TRUNC('WEEK', last_restock) AS last_restock_period,
        ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock < '2025-08-01'
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_months, id) AS event_seq_2,
        ROW_NUMBER() OVER w_grp_2 AS event_seq_3
    FROM base
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY last_restock_period, id), w_grp_2 AS (PARTITION BY product_id ORDER BY last_restock, id)
),
with_windows AS (
    SELECT
        id,
        product_id,
        last_restock,
        last_restock_period,
        last_restock_plus_months,
        event_seq,
        row_seq,
        event_seq_2,
        event_seq_3,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY event_seq_3 DESC NULLS FIRST) AS event_seq_3_cume_dist,
        SUM(event_seq_3) OVER (PARTITION BY product_id ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq_3,
        COUNT(event_seq_3) OVER (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS event_seq_3_count_90d
    FROM scored
),
with_windows_inventory AS (
    SELECT
        *,
        FIRST_VALUE(event_seq_3_count_90d) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY last_restock_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq_3_count_90d,
        SUM(event_seq) OVER (PARTITION BY product_id ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS event_seq_sum_7d,
        SUM(running_event_seq_3) OVER w_running AS cumulative_running_event_seq_3,
        MAX(event_seq_3_cume_dist) OVER w_recent AS event_seq_3_cume_dist_max_30d,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY row_seq, id) AS row_seq_bucket
    FROM with_windows
    WINDOW w_running AS (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY product_id ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    product_id,
    last_restock,
    last_restock_period,
    last_restock_plus_months,
    event_seq,
    event_seq_2,
    event_seq_3,
    event_seq_3_cume_dist
FROM with_windows_inventory
ORDER BY event_seq_2 DESC;
