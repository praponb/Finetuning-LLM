WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.amount < 1000
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(age) OVER w_part AS running_max_age,
        AVG(age) OVER w_part AS running_avg_age,
        SUM(age) OVER w_part AS running_sum_age,
        COUNT(age) OVER w_part AS running_count_age
    FROM eligible
    WHERE MONTH(signup_date) IN (1, 5, 11)
    WINDOW w_part AS (PARTITION BY country ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.city,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.running_avg_age,
        s.running_sum_age,
        s.running_count_age,
        x.account_account_type
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY running_avg_age ASC, id ASC) AS dedup_rank
    FROM with_last
    QUALIFY dedup_rank = 1
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.city,
        s.signup_date,
        s.is_active,
        s.running_count_age,
        s.account_account_type,
        s.dedup_rank,
        x.account_account_type_2,
        x.balance
    FROM latest s
    JOIN LATERAL (
            SELECT c.account_type AS account_account_type_2, c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        city,
        is_active,
        running_count_age,
        account_account_type,
        balance,
        NTILE(3) OVER (PARTITION BY account_account_type, city ORDER BY balance ASC, id ASC) AS balance_bucket,
        LEAD(balance, 1) OVER w_grp AS next_balance
    FROM with_latest
    WINDOW w_grp AS (PARTITION BY city ORDER BY running_count_age ASC, id ASC)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        city,
        is_active,
        account_account_type,
        balance,
        balance_bucket,
        next_balance
    FROM with_windows_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, account_account_type ORDER BY next_balance ASC, id ASC) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_account_type,
        city,
        next_balance
    FROM ranked
) src
PIVOT (
    MAX(next_balance) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('Tokyo' AS tokyo, 'Chiang Mai' AS chiang_mai)
);
