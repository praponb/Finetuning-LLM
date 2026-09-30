WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        RANK() OVER (PARTITION BY order_id, status ORDER BY cost DESC NULLS LAST) AS cost_rank,
        COUNT(cost) OVER (PARTITION BY carrier, status) AS carrier_count_cost,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2
    FROM shipments
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        shipped_date,
        cost,
        status,
        cost_rank,
        carrier_count_cost,
        cost_avg_2,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date,
        ROUND(cost_avg_2 / cost, 2) AS cost_avg_2_per_cost
    FROM with_windows
    WHERE shipped_date >= TO_DATE('2023-07-27')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        status,
        SUM(cost_avg_2_per_cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost_avg_2_per_cost,
        MIN(cost_avg_2_per_cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost_avg_2_per_cost,
        MAX(cost_avg_2_per_cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost_avg_2_per_cost,
        COUNT(cost_avg_2_per_cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost_avg_2_per_cost,
        ROW_NUMBER() OVER (ORDER BY cost_avg_2_per_cost, id) AS row_seq
    FROM base
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_ADD(shipped_date, 78) AS shipped_date_plus_78d,
        ROUND(running_count_cost_avg_2_per_cost * 1.07, 2) AS running_count_cost_avg_2_per_cost_scaled
    FROM scored
    WHERE shipped_date < '2024-05-01'
)
SELECT
    id,
    shipped_date,
    status,
    running_min_cost_avg_2_per_cost,
    running_max_cost_avg_2_per_cost,
    running_count_cost_avg_2_per_cost,
    shipped_date_plus_78d,
    running_count_cost_avg_2_per_cost_scaled
FROM cleaned;
