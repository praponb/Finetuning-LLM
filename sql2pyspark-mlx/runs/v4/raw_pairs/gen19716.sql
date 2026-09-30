SELECT
    *,
    LAST_VALUE(monthly_fee) IGNORE NULLS OVER w_hist AS last_known_monthly_fee,
    ROW_NUMBER() OVER w_hist_2 AS seq_no,
    ROW_NUMBER() OVER w_hist_2 AS row_seq
FROM subscriptions
WINDOW w_hist AS (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY status ORDER BY monthly_fee DESC, id)
