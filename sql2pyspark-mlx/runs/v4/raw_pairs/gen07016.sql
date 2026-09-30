WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        ROUND(monthly_fee * 0.9, 2) monthly_fee_adjusted
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee_adjusted DESC, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(monthly_fee_adjusted) OVER (PARTITION BY plan, customer_id) plan_sum_monthly_fee_adjusted,
        AVG(monthly_fee_adjusted) OVER w_running monthly_fee_adjusted_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY latest_rank, id) seq_no,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee
    FROM latest
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY latest_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
latest_subscriptions AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY start_date, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
metrics_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(monthly_fee_adjusted_avg_3_prior) OVER (ORDER BY plan_sum_monthly_fee_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee_adjusted_avg_3_prior,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY status, customer_id) status_stddev_samp_seq_no
    FROM latest_subscriptions
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics_subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY row_num DESC, id) = 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        customer_id,
        CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
        PERCENTILE_APPROX(row_seq, 0.95) AS row_seq_p95,
        SUM(CASE WHEN status = 'CANCELLED' THEN cumulative_monthly_fee_adjusted_avg_3_prior ELSE 0 END) AS cancelled_cumulative_monthly_fee_adjusted_avg_3_prior,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50
    FROM deduped
    GROUP BY customer_id, CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    s.customer_id,
    s.seq_no_tier,
    s.row_seq_p95,
    s.cancelled_cumulative_monthly_fee_adjusted_avg_3_prior,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM daily s
ORDER BY cancelled_cumulative_monthly_fee_adjusted_avg_3_prior NULLS LAST
