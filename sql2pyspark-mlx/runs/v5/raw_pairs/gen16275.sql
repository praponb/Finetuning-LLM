WITH filtered AS (
    SELECT *, DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time > TIMESTAMP '2023-07-02 12:00:00'
)
SELECT
    id,
    txn_time_label,
    RANK() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS FIRST) AS amount_rank,
    LAST_VALUE(amount, TRUE) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    AVG(amount) OVER (PARTITION BY account_id ORDER BY amount ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share
FROM filtered
WHERE account_id = 31;
