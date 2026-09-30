WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM accounts
    WHERE account_type = 'CREDIT'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount > 1500
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        status,
        opened_date_label,
        LEAD(balance) OVER w_part AS next_balance,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM not_flagged
    WHERE account_type <> 'SAVINGS'
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY opened_date, id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date_label,
        next_balance,
        seq_no,
        FIRST_VALUE(next_balance, TRUE) OVER w_grp AS first_known_next_balance,
        RANK() OVER (PARTITION BY status ORDER BY seq_no DESC) AS seq_no_rank,
        FIRST_VALUE(next_balance, TRUE) OVER (PARTITION BY status ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_balance_2,
        LEAD(next_balance, 2) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_next_balance,
        RANK() OVER w_ordered AS next_balance_rank
    FROM with_windows
    WINDOW w_grp AS (ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY status ORDER BY next_balance DESC NULLS LAST)
)
SELECT
    id,
    next_balance,
    first_known_next_balance,
    seq_no_rank,
    first_known_next_balance_2
FROM metrics
