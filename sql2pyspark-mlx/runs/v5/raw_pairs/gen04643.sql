WITH valid AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), txn_time) days_since_txn_time,
        COALESCE(merchant, 'N/A') resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
)
SELECT
    *,
    COUNT(amount) OVER w_recent merchant_count_amount,
    AVG(days_since_txn_time) OVER (ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) days_since_txn_time_avg_2_prior,
    CUME_DIST() OVER (PARTITION BY account_id ORDER BY days_since_txn_time DESC NULLS LAST) days_since_txn_time_cume_dist
FROM valid
WINDOW w_recent AS (PARTITION BY merchant)
