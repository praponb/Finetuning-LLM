WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        ROUND(balance, 2) balance_rounded
    FROM accounts
    WHERE opened_date >= DATE '2023-04-04'
      AND MONTH(opened_date) IN (5, 7, 9)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, account_type
    FROM filtered
    INTERSECT
    SELECT customer_id, account_type
    FROM accounts
    WHERE YEAR(opened_date) = 2023
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.account_type = k.account_type
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (ORDER BY balance_rounded DESC NULLS FIRST) balance_rounded_dense_rank
    FROM verified
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance_rounded,
        balance_rounded_dense_rank,
        RANK() OVER (PARTITION BY account_type, customer_id ORDER BY balance_rounded) balance_rounded_rank,
        RANK() OVER (PARTITION BY customer_id, account_type ORDER BY balance_rounded_dense_rank DESC NULLS LAST) balance_rounded_dense_rank_rank
    FROM enriched
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.account_type,
        e.balance_rounded_dense_rank,
        e.balance_rounded_rank,
        c.email,
        c.name
    FROM with_windows e
    INNER JOIN customers c
        ON e.customer_id = c.id
),
filtered_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        balance_rounded_rank,
        email,
        name,
        balance_rounded_rank + balance_rounded_dense_rank balance_rounded_rank_plus_balance_rounded_dense_rank
    FROM joined
    WHERE name LIKE '%MILL%'
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count,
    ROUND(AVG(balance_rounded_rank), 2) AS avg_balance_rounded_rank,
    ROUND(AVG(balance_rounded_rank), 2) AS avg_balance_rounded_rank_2,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(*) AS cnt_2
FROM filtered_accounts
GROUP BY customer_id
ORDER BY cnt DESC NULLS LAST
