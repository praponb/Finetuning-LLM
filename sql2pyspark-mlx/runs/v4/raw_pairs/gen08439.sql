WITH cleaned AS (
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE merchant IS NOT NULL
),
joined AS (
    SELECT
        src.*,
        a.status,
        a.account_type
    FROM cleaned src
    JOIN accounts a
        ON src.account_id = a.id
),
metrics AS (
    SELECT
        *,
        SUM(amount) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_type), 4) AS amount_share
    FROM joined
)
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    CASE WHEN amount_share >= 0.73 THEN 'HIGH' ELSE 'LOW' END AS amount_share_tier,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE txn_type = 'FEE') AS fee_rows,
    MIN(amount_avg_3) AS min_amount_avg_3,
    SUM(CASE WHEN account_type = 'CHECKING' THEN running_amount ELSE 0 END) AS checking_running_amount,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY week_start, activity_month, amount_share_tier
HAVING min_amount_avg_3 >= 0
