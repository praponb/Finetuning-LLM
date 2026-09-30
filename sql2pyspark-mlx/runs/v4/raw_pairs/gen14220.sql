WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        ROUND(amount, 2) amount_rounded,
        DATE_FORMAT(txn_time, 'MMM yyyy') txn_time_label
    FROM transactions
    WHERE merchant IS NOT NULL
      AND is_flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        amount_rounded,
        txn_time_label,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_avg_7d,
        LAST_VALUE(amount_rounded) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount_rounded
    FROM filtered
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.account_type,
        p.status,
        p.balance
    FROM with_windows e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
filtered_transactions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(amount_avg_7d, amount_rounded) least_amount_avg_7d_amount_rounded,
        ROUND(balance * 0.85, 2) adjusted_balance
    FROM joined
    WHERE txn_time > DATE '2024-01-13'
      AND amount_avg_7d < 1379
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY amount_avg_7d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) balance_avg_6,
        LEAD(amount_avg_7d, 1) OVER (PARTITION BY txn_type ORDER BY last_known_amount_rounded, id) next_amount_avg_7d,
        SUM(amount_rounded) OVER w_running running_amount_rounded,
        SUM(amount_avg_7d) OVER (PARTITION BY txn_type, account_type) txn_type_sum_amount_avg_7d,
        NTILE(4) OVER w_grp adjusted_balance_bucket
    FROM filtered_transactions
    WINDOW w_running AS (PARTITION BY status ORDER BY amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY account_id, status ORDER BY adjusted_balance, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY account_id ORDER BY balance_avg_6 DESC, id) balance_avg_6_bucket,
        PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount_avg_7d DESC NULLS LAST) amount_avg_7d_percent_rank
    FROM scored
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_type,
            txn_type,
            balance
        FROM sessions_transactions
    ) src
    PIVOT (
        SUM(balance) AS amount,
        COUNT(1) AS cnt
        FOR txn_type IN ('FEE', 'CREDIT')
    )
)
SELECT
    account_type,
    FEE_amount,
    CREDIT_amount,
    CREDIT_cnt,
    CASE WHEN account_type RLIKE '^(SAVINGS|CREDIT)$' THEN 'EXTENDED' ELSE 'CORE' END AS account_type_zone
FROM pivoted
