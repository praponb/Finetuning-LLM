SELECT
    *,
    NTILE(10) OVER w_recent AS monthly_fee_bucket,
    CUME_DIST() OVER w_hist AS monthly_fee_cume_dist,
    PERCENT_RANK() OVER (ORDER BY monthly_fee DESC) AS monthly_fee_percent_rank,
    AVG(monthly_fee) OVER w_part AS monthly_fee_avg_2_prior,
    AVG(monthly_fee) OVER w_part AS monthly_fee_avg_2_prior_2
FROM subscriptions
WINDOW w_recent AS (ORDER BY monthly_fee, id), w_hist AS (PARTITION BY plan ORDER BY monthly_fee DESC), w_part AS (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
