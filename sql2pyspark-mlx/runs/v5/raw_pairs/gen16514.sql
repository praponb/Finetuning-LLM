WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(opened_date, 1) AS opened_date_plus_months
    FROM accounts
    WHERE opened_date > DATE '2023-08-10'
      AND status IS DISTINCT FROM 'CLOSED'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        MAX(balance) OVER (PARTITION BY account_type ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance,
        MIN(balance) OVER (PARTITION BY account_type ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3
    FROM filtered
    WHERE balance < 44000
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        running_min_balance,
        balance_avg_3,
        DENSE_RANK() OVER (PARTITION BY account_type, customer_id ORDER BY balance_avg_3 DESC NULLS FIRST) AS balance_avg_3_dense_rank,
        SUM(running_min_balance) OVER (ORDER BY running_max_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_min_balance,
        NTILE(4) OVER (ORDER BY running_avg_balance DESC, id) AS running_avg_balance_bucket
    FROM scored
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_avg_3_dense_rank NULLS FIRST, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_type,
        balance_avg_3,
        balance_avg_3_dense_rank
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance_avg_3_dense_rank DESC, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance_avg_3_dense_rank,
        LEAD(balance_avg_3) OVER (PARTITION BY account_type ORDER BY balance_avg_3_dense_rank, id) AS next_balance_avg_3,
        LEAD(balance_avg_3_dense_rank) OVER (PARTITION BY account_type ORDER BY balance_avg_3_dense_rank, id) AS next_balance_avg_3_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_avg_3_dense_rank, id) AS seq_no
    FROM ranked
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT
    SELECT id
    FROM accounts
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Running and moving aggregates
SELECT
    balance_avg_3_dense_rank,
    next_balance_avg_3,
    next_balance_avg_3_dense_rank,
    seq_no,
    PERCENT_RANK() OVER (ORDER BY seq_no DESC NULLS FIRST) AS seq_no_percent_rank,
    CUME_DIST() OVER (ORDER BY next_balance_avg_3_dense_rank NULLS LAST) AS next_balance_avg_3_dense_rank_cume_dist,
    DENSE_RANK() OVER (ORDER BY balance_avg_3_dense_rank DESC) AS balance_avg_3_dense_rank_dense_rank,
    DENSE_RANK() OVER (ORDER BY balance_avg_3_dense_rank DESC NULLS FIRST) AS balance_avg_3_dense_rank_dense_rank_2,
    DENSE_RANK() OVER (ORDER BY next_balance_avg_3 DESC) AS next_balance_avg_3_dense_rank_2
FROM kept_rows;
