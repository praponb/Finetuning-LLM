WITH filtered AS (
    SELECT
        *,
        COALESCE(balance, 0) balance_filled,
        COALESCE(status, 'N/A') resolved_status
    FROM accounts
    WHERE status IS NOT NULL
      AND customer_id IS NOT NULL
)
SELECT
    id,
    customer_id,
    account_type,
    opened_date,
    balance_filled,
    LEAD(balance_filled, 1) OVER (ORDER BY balance, id) next_balance_filled,
    RANK() OVER w_running balance_filled_rank,
    SUM(balance_filled) OVER (PARTITION BY account_type, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance_filled,
    ROW_NUMBER() OVER (ORDER BY balance DESC, id) row_seq,
    LAST_VALUE(status) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status
FROM filtered
WINDOW w_running AS (PARTITION BY status ORDER BY balance_filled DESC);
