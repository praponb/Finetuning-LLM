WITH cleaned AS (
    SELECT *, ABS(balance) AS abs_balance
    FROM accounts
    WHERE opened_date >= DATE '2023-03-12'
      AND status IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY opened_date NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
matched_keys AS (
    SELECT status, account_type
    FROM eligible
    EXCEPT ALL
    SELECT status, account_type
    FROM accounts
    WHERE account_type = 'SAVINGS'
),
reconciled AS (
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.status = k.status AND s.account_type = k.account_type
),
joined AS (
    SELECT
        a.*,
        c.email,
        c.age,
        c.credit_limit
    FROM reconciled a
    JOIN customers c
        ON a.customer_id = c.id
    WHERE c.age < 58
)
SELECT
    d.country,
    d.city,
    COUNT(*) AS num_rows,
    SUM(a.age) AS total_age
FROM joined a
LEFT JOIN customers d ON a.customer_id = d.id
GROUP BY ROLLUP (d.country, d.city);
