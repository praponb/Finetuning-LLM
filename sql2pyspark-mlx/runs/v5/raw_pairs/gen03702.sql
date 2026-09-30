WITH scored AS (
    SELECT
        *,
        CUME_DIST() OVER w_part AS monthly_fee_cume_dist,
        DENSE_RANK() OVER w_grp AS monthly_fee_dense_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
        SUM(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS seq_no
    FROM subscriptions
    WINDOW w_part AS (ORDER BY monthly_fee DESC NULLS FIRST), w_grp AS (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST)
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee_cume_dist,
    monthly_fee_dense_rank,
    running_monthly_fee,
    CASE WHEN plan RLIKE '^(basic|premium)$' THEN 'EXTENDED' WHEN plan RLIKE '^(standard|enterprise)$' THEN 'GROUP_B' ELSE 'CORE' END AS plan_zone
FROM scored
ORDER BY monthly_fee_cume_dist DESC;
