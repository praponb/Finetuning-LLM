WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        MONTH(txn_time) txn_time_month,
        COALESCE(merchant, 'UNKNOWN') merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND is_flagged = TRUE
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.merchant,
        s.merchant_filled,
        a.account_type,
        a.opened_date
    FROM filtered s
    LEFT JOIN accounts a
        ON s.account_id = a.id
    WHERE a.account_type != 'SAVINGS'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY opened_date NULLS LAST, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_amount,
        MIN(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_amount,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_amount,
        LEAD(amount, 1) OVER (PARTITION BY account_type, account_id ORDER BY dedup_rank, id) next_amount
    FROM ranked
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last7_sum_amount >= 16600 THEN 'LARGE' ELSE 'SMALL' END AS last7_sum_amount_tier,
    COUNT(*) FILTER (WHERE merchant = 'Shell') AS shell_rows,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    COUNT(*) FILTER (WHERE account_type = 'CHECKING') AS checking_rows,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    SUM(CASE WHEN account_type = 'CHECKING' THEN next_amount ELSE 0 END) AS checking_next_amount
FROM enriched
GROUP BY CASE WHEN last7_sum_amount >= 16600 THEN 'LARGE' ELSE 'SMALL' END;
