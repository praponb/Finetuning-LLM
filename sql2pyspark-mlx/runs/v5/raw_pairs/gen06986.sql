WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        ROUND(balance, 1) AS balance_rounded,
        INITCAP(TRIM(status)) AS status_title
    FROM accounts
    WHERE status = 'FROZEN'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance_rounded, 1, 0) OVER (PARTITION BY status ORDER BY balance_rounded, id) AS prev_balance_rounded,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_type, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        FIRST_VALUE(balance, TRUE) OVER w_ordered AS first_known_balance
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY account_type ORDER BY balance_rounded DESC NULLS LAST) = 1
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(first_known_balance) OVER (PARTITION BY account_type, status ORDER BY balance_rounded, id) AS next_first_known_balance,
        LAG(first_known_balance) OVER (PARTITION BY account_type, status ORDER BY balance_rounded, id) AS prev_first_known_balance,
        LEAD(balance) OVER (PARTITION BY account_type, status ORDER BY balance_rounded, id) AS next_balance,
        LAG(balance) OVER (PARTITION BY account_type, status ORDER BY balance_rounded, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY balance_rounded, id) AS event_seq
    FROM deduped
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        prev_balance_rounded,
        prev_first_known_balance
    FROM with_windows_accounts
) src
UNPIVOT (
    val FOR measure IN (prev_balance_rounded AS PREV_BALANCE_ROUNDED, prev_first_known_balance)
);
