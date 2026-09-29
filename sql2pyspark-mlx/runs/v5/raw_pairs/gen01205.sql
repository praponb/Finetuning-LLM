WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(end_date, 66) end_date_plus_66d
    FROM subscriptions
    WHERE start_date > DATE '2023-04-16'
      AND customer_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC NULLS LAST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM latest
    EXCEPT
    SELECT status
    FROM subscriptions
    WHERE start_date <= '2023-06-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.status = k.status
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) monthly_fee_share,
        MAX(monthly_fee) OVER (PARTITION BY customer_id, plan) customer_id_max_monthly_fee,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY status) status_stddev_samp_monthly_fee
    FROM verified
),
enriched AS (
    -- Ranking and gap features
    SELECT
        end_date,
        status,
        end_date_plus_66d,
        rank_in_group,
        DENSE_RANK() OVER (ORDER BY monthly_fee DESC) monthly_fee_dense_rank,
        SUM(monthly_fee) OVER (PARTITION BY status) status_sum_monthly_fee,
        MIN(status_stddev_samp_monthly_fee) OVER (PARTITION BY customer_id) customer_id_min_status_stddev_samp_monthly_fee
    FROM with_windows
),
base_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        end_date,
        status,
        end_date_plus_66d,
        rank_in_group,
        monthly_fee_dense_rank,
        status_sum_monthly_fee,
        customer_id_min_status_stddev_samp_monthly_fee,
        COALESCE(customer_id_min_status_stddev_samp_monthly_fee, 0) customer_id_min_status_stddev_samp_monthly_fee_filled
    FROM enriched
    WHERE customer_id_min_status_stddev_samp_monthly_fee >= 186
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(customer_id_min_status_stddev_samp_monthly_fee_filled) OVER (PARTITION BY status) status_count_customer_id_min_status_stddev_samp_monthly_fee_filled,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY customer_id_min_status_stddev_samp_monthly_fee_filled NULLS FIRST) customer_id_min_status_stddev_samp_monthly_fee_filled_percent_rank,
        ROUND(customer_id_min_status_stddev_samp_monthly_fee / SUM(customer_id_min_status_stddev_samp_monthly_fee) OVER (PARTITION BY status), 4) customer_id_min_status_stddev_samp_monthly_fee_share
    FROM base_subscriptions
),
daily AS (
    -- Multi-level aggregation
    SELECT
        status,
        COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_rows,
        COUNT(*) FILTER (WHERE status = 'PAUSED') AS paused_rows
    FROM enriched_subscriptions
    GROUP BY status
)
SELECT
    status,
    expired_rows,
    paused_rows,
    CASE WHEN status RLIKE '^(ACTIVE|CANCELLED)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone
FROM daily
ORDER BY status_zone, status;
