WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN balance >= 66000 THEN 'High' WHEN balance >= 27000 THEN 'Medium' ELSE 'Low' END AS balance_level
    FROM accounts
    WHERE status IS NOT NULL
      AND customer_id IN (3, 27, 28)
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, p.name
    FROM base e
    JOIN customers p
        ON e.customer_id = p.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST) = 1
),
joined_accounts AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.name AS customer_name,
        d.segment,
        d.city
    FROM deduped e
    JOIN customers d
        ON e.customer_id = d.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(balance) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        SUM(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        FIRST_VALUE(segment, TRUE) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment
    FROM kept
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        name,
        customer_name,
        segment,
        last_known_balance,
        running_balance,
        first_known_segment,
        running_balance - balance AS running_balance_minus_balance,
        TRIM(segment) AS clean_segment
    FROM metrics
    WHERE opened_date > TO_DATE('2024-04-17')
      AND customer_id IS NOT NULL
      AND customer_name IS NOT NULL
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    name,
    customer_name,
    last_known_balance,
    running_balance
FROM cleaned
ORDER BY account_type DESC, last_known_balance;
