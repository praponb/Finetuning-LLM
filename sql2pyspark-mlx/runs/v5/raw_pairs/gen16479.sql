SELECT
    *,
    SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_monthly_fee,
    MAX(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_monthly_fee,
    MIN(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share
FROM subscriptions;
