WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        LOWER(merchant) AS merchant_lower,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-04-19 07:00:00'
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.resolved_merchant,
        b.balance,
        b.account_type
    FROM base src
    INNER JOIN accounts b
        ON src.account_id = b.id
    WHERE b.balance <= 30000
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(balance, 2, 0) OVER (PARTITION BY account_id ORDER BY balance, id) AS next_balance,
        NTILE(4) OVER (PARTITION BY account_type ORDER BY balance, id) AS balance_bucket,
        SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
    FROM enriched
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        resolved_merchant,
        running_balance,
        AVG(next_balance) OVER (PARTITION BY account_type, account_id ORDER BY balance_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_balance_avg_5_prior,
        AVG(next_balance) OVER w_grp AS next_balance_avg_6_prior
    FROM scored
    WINDOW w_grp AS (PARTITION BY account_id ORDER BY balance_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_balance >= 382403 THEN 'LARGE' ELSE 'SMALL' END AS running_balance_tier,
    APPROX_PERCENTILE(running_balance, ARRAY(0.25, 0.5, 0.75)) AS running_balance_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(next_balance_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS next_balance_avg_5_prior_quartiles,
    ROUND(AVG(next_balance_avg_6_prior), 2) AS avg_next_balance_avg_6_prior
FROM scored_transactions
GROUP BY CASE WHEN running_balance >= 382403 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY running_balance_tier DESC NULLS LAST;
