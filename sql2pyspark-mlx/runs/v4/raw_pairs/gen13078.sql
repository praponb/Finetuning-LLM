WITH filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        ROUND(balance, 0) balance_rounded
    FROM accounts
    WHERE customer_id = 25
),
enriched AS (
    SELECT
        *,
        LEAD(balance_rounded, 1) OVER (PARTITION BY status ORDER BY balance, id) next_balance_rounded,
        LAST_VALUE(balance_rounded) IGNORE NULLS OVER (PARTITION BY status ORDER BY balance_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_balance_rounded,
        AVG(balance_rounded) OVER (PARTITION BY account_type) account_type_avg_balance_rounded
    FROM filtered
),
with_dims AS (
    SELECT
        s.*,
        c.segment,
        c.age,
        c.email
    FROM enriched s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.status,
    s.next_balance_rounded,
    s.last_known_balance_rounded,
    s.account_type_avg_balance_rounded,
    s.age,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM with_dims s
ORDER BY last_known_balance_rounded DESC, account_type
