WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY txn_time DESC, id) AS rank_in_group
        FROM transactions
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    account_id,
    SUM(amount) AS total_amount,
    MAX(txn_time) AS ended_at,
    GROUPING(account_id) AS grouping_id
FROM latest
GROUP BY ROLLUP (account_id)
HAVING total_amount < 0
