WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM subscriptions
    WHERE start_date > DATE '2023-12-30'
      AND customer_id IS NOT NULL
      AND end_date <= '2025-08-01'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM filtered
    INTERSECT
    SELECT customer_id, id
    FROM subscriptions
    WHERE start_date >= '2023-05-12'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
        AVG(monthly_fee) OVER (PARTITION BY status) AS status_avg_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id, plan) AS customer_id_sum_monthly_fee
    FROM verified
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    status_title,
    monthly_fee_share,
    status_avg_monthly_fee,
    customer_id_sum_monthly_fee,
    COUNT(monthly_fee_share) OVER w_grp AS customer_id_count_monthly_fee_share,
    STDDEV_SAMP(customer_id_sum_monthly_fee) OVER w_recent AS plan_stddev_samp_customer_id_sum_monthly_fee,
    DENSE_RANK() OVER w_ordered AS monthly_fee_share_dense_rank
FROM metrics
WINDOW w_grp AS (PARTITION BY customer_id), w_recent AS (PARTITION BY plan), w_ordered AS (ORDER BY monthly_fee_share DESC);
