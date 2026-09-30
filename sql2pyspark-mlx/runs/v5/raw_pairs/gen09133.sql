WITH valid AS (
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        COALESCE(balance, 0) AS balance_filled,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM accounts
    WHERE opened_date > TO_DATE('2023-06-17')
      AND status <> 'CLOSED'
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < '2023-10-01'
        )
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance_filled DESC, id) AS row_seq
    FROM not_flagged
),
enriched AS (
    SELECT
        id,
        opened_date,
        balance_filled,
        status_clean,
        AVG(balance_filled) OVER w_recent AS running_avg_balance_filled,
        SUM(balance_filled) OVER w_recent AS running_sum_balance_filled,
        MAX(balance_filled) OVER w_recent AS running_max_balance_filled
    FROM metrics
    WHERE row_seq IS NOT NULL
    WINDOW w_recent AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN balance_filled >= 68 THEN 'HIGH' ELSE 'LOW' END AS balance_filled_tier,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(balance_filled, ARRAY(0.25, 0.5, 0.75)) AS balance_filled_quartiles,
    APPROX_PERCENTILE(running_max_balance_filled, ARRAY(0.25, 0.5, 0.75)) AS running_max_balance_filled_quartiles
FROM enriched
GROUP BY 1, 2
ORDER BY quarter_start
