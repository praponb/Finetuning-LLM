WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        CAST(cost AS INT) cost_int,
        DATE_TRUNC('MONTH', delivered_date) delivered_date_period
    FROM shipments
    WHERE delivered_date >= DATE '2023-03-26'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        shipped_date,
        status,
        cost_int,
        delivered_date_period,
        MIN(cost_int) OVER w_hist last4_min_cost_int,
        COUNT(cost_int) OVER w_hist last4_count_cost_int,
        SUM(cost_int) OVER w_hist last4_sum_cost_int
    FROM base
    WHERE delivered_date_period < '2023-04-14'
    WINDOW w_hist AS (PARTITION BY status ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY carrier, status ORDER BY delivered_date_period DESC NULLS LAST) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(last4_count_cost_int) OVER w_recent running_last4_count_cost_int,
        LAG(last4_count_cost_int, 1) OVER (PARTITION BY status ORDER BY last4_min_cost_int, id) prev_last4_count_cost_int,
        MAX(last4_count_cost_int) OVER w_grp status_max_last4_count_cost_int
    FROM deduped
    WINDOW w_recent AS (ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY status)
)
SELECT
    id,
    carrier,
    shipped_date,
    status,
    last4_min_cost_int,
    last4_count_cost_int,
    row_num,
    CASE WHEN status RLIKE '^(DELIVERED|LOST)$' THEN 'PRIORITY' ELSE 'CORE' END AS status_zone
FROM with_windows_shipments;
