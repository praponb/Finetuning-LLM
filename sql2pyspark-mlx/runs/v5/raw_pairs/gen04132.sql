WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(balance, 0) balance_rounded,
        COALESCE(status, 'NONE') resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-05-01'
      AND balance > 41000
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY status ORDER BY balance_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status,
        SUM(balance_rounded) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance_rounded,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_rounded DESC, id) seq_no
    FROM base
    WHERE balance IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.account_type,
        a.balance,
        a.opened_date,
        a.status,
        a.balance_rounded,
        a.cumulative_balance_rounded,
        a.row_seq,
        a.seq_no,
        b.age
    FROM scored a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance,
        status,
        cumulative_balance_rounded,
        row_seq,
        seq_no,
        age,
        SUM(row_seq) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_seq,
        AVG(seq_no) OVER (PARTITION BY status ORDER BY balance_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) seq_no_avg_3
    FROM joined
),
metrics_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        row_seq,
        age,
        cumulative_row_seq,
        LAST_VALUE(balance) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_balance,
        LAST_VALUE(seq_no_avg_3, TRUE) OVER (PARTITION BY customer_id ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_seq_no_avg_3,
        SUM(row_seq) OVER (PARTITION BY customer_id, status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_seq_2
    FROM metrics
),
stats AS (
    -- Multi-level aggregation
    SELECT
        customer_id,
        SUM(last_known_balance) AS total_last_known_balance,
        COUNT(*) AS num_rows,
        COUNT(*) AS num_rows_2,
        ROUND(AVG(cumulative_row_seq_2), 2) AS avg_cumulative_row_seq_2,
        COUNT(*) AS cnt
    FROM metrics_accounts
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_last_known_balance,
    num_rows,
    num_rows_2,
    avg_cumulative_row_seq_2,
    cnt
FROM stats
ORDER BY cnt DESC
