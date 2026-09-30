WITH valid AS (
    SELECT
        *,
        CONCAT(status, ' ', account_type) AS status_account_type,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM accounts
    WHERE status IS DISTINCT FROM 'FROZEN'
      AND customer_id = 5
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date NULLS LAST, id) = 1
),
with_windows AS (
    SELECT
        *,
        LEAD(balance, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_balance,
        SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        LEAD(balance, 2) OVER (PARTITION BY status ORDER BY balance, id) AS next_balance_2,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS seq_no
    FROM deduped
),
cleaned AS (
    SELECT
        *,
        ROUND(next_balance, 2) AS next_balance_rounded,
        COALESCE(status, 'OTHER') AS status_filled
    FROM with_windows
    WHERE opened_date >= DATE '2023-03-02'
      AND status IS NULL
)
SELECT
    d.city,
    s.status,
    COUNT(*) AS num_rows,
    SUM(s.seq_no) AS total_seq_no,
    GROUPING_ID() AS grouping_level
FROM cleaned s
INNER JOIN customers d ON s.customer_id = d.id
GROUP BY ROLLUP (d.city, s.status)
ORDER BY city NULLS LAST;
