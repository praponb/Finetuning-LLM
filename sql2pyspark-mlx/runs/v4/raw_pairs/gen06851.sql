WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(start_date, 6) AS start_date_minus_6d
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-08-29')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share,
        LAG(monthly_fee, 2) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee, id) AS prev_monthly_fee,
        LAG(monthly_fee, 2, 0) OVER (PARTITION BY plan ORDER BY start_date_minus_6d, id) AS prev_monthly_fee_2
    FROM base
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_share DESC NULLS LAST) AS monthly_fee_share_percent_rank,
        LEAD(prev_monthly_fee_2, 1) OVER w_part AS next_prev_monthly_fee_2,
        DENSE_RANK() OVER w_running AS prev_monthly_fee_dense_rank
    FROM with_windows
    WINDOW w_part AS (PARTITION BY status, customer_id ORDER BY start_date_minus_6d, id), w_running AS (PARTITION BY customer_id ORDER BY prev_monthly_fee DESC NULLS LAST)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM scored
    EXCEPT
    SELECT id, plan
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(next_prev_monthly_fee_2 / SUM(next_prev_monthly_fee_2) OVER (PARTITION BY customer_id), 4) AS next_prev_monthly_fee_2_share,
        MIN(next_prev_monthly_fee_2) OVER w_hist AS plan_min_next_prev_monthly_fee_2,
        MIN(prev_monthly_fee) OVER w_hist AS plan_min_prev_monthly_fee,
        DENSE_RANK() OVER (PARTITION BY status, plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_dense_rank
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY plan)
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            plan,
            monthly_fee_dense_rank
        FROM enriched
    ) src
    PIVOT (
        AVG(monthly_fee_dense_rank) AS amount,
        COUNT(1) AS cnt
        FOR plan IN ('enterprise' AS enterprise, 'basic' AS basic)
    )
)
SELECT
    status,
    enterprise_amount,
    basic_amount,
    basic_cnt,
    CASE WHEN status RLIKE '^(CANCELLED|EXPIRED)$' THEN 'GROUP_A' WHEN status RLIKE '^(ACTIVE|PAUSED)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS status_zone
FROM by_type
ORDER BY basic_cnt, status_zone DESC;
