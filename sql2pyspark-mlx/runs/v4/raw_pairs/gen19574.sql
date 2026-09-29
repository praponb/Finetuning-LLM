WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_monthly_fee,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        PERCENT_RANK() OVER w_grp AS monthly_fee_percent_rank
    FROM subscriptions
    WINDOW w_grp AS (PARTITION BY status ORDER BY monthly_fee NULLS FIRST)
)
SELECT
    id,
    customer_id,
    plan,
    monthly_fee,
    status,
    customer_id_stddev_samp_monthly_fee
FROM metrics
ORDER BY customer_id_stddev_samp_monthly_fee DESC NULLS FIRST, status DESC
