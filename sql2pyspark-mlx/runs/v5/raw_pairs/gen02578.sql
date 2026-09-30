WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance,
        AVG(balance) OVER (PARTITION BY customer_id, status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3,
        LEAD(balance, 1, 0) OVER (PARTITION BY status, account_type ORDER BY opened_date, id) AS next_balance,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY status, account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status
    FROM accounts
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY next_balance, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
deduped_accounts AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY rank_in_group, id) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(balance) OVER (PARTITION BY customer_id ORDER BY dedup_rank, id) AS next_balance_2,
        LAG(balance) OVER (PARTITION BY customer_id ORDER BY dedup_rank, id) AS prev_balance,
        LEAD(balance_avg_3) OVER (PARTITION BY customer_id ORDER BY dedup_rank, id) AS next_balance_avg_3,
        LAG(balance_avg_3) OVER (PARTITION BY customer_id ORDER BY dedup_rank, id) AS prev_balance_avg_3,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dedup_rank, id) AS event_seq,
        SUM(next_balance) OVER (PARTITION BY customer_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_balance
    FROM deduped_accounts
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_next_balance >= 303052 THEN 'LARGE' ELSE 'SMALL' END AS running_next_balance_tier,
        MAX(next_balance) AS max_next_balance,
        STDDEV_SAMP(balance) AS balance_stddev
    FROM enriched
    GROUP BY running_next_balance_tier
)
SELECT
    running_next_balance_tier,
    max_next_balance,
    balance_stddev
FROM summary;
