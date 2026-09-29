WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        SUM(amount) OVER w_hist AS cumulative_amount,
        AVG(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
    FROM transactions
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    merchant,
    is_flagged,
    first_known_amount,
    amount_avg_5_prior,
    CASE WHEN merchant RLIKE '^(Amazon|Lazada)$' THEN 'CORE' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS merchant_zone
FROM enriched
ORDER BY amount_avg_5_prior DESC;
