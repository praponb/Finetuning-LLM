WITH kept_keys AS (
    SELECT customer_id, id
    FROM accounts
    EXCEPT
    SELECT customer_id, id
    FROM accounts
    WHERE opened_date < '2023-11-01'
),
verified AS (
    SELECT s.*
    FROM accounts s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
valid AS (
    SELECT
        *,
        CONCAT(account_type, ' ', status) AS account_type_status,
        COALESCE(status, 'N/A') AS resolved_status
    FROM verified
    WHERE opened_date >= DATE '2023-08-16'
      AND YEAR(opened_date) >= 2025
),
cleaned AS (
    SELECT
        *,
        TRIM(account_type) AS clean_account_type,
        DATE_SUB(opened_date, 26) AS opened_date_minus_26d,
        COALESCE(status, 'OTHER') AS status_clean
    FROM valid
    WHERE opened_date >= TO_DATE('2023-06-14')
      AND status IS NOT NULL
),
joined AS (
    SELECT
        s.*,
        b.segment,
        b.city
    FROM cleaned s
    JOIN customers b
        ON s.customer_id = b.id
    WHERE b.age BETWEEN 55 AND 56
),
kept AS (
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date_minus_26d
        )
),
with_windows AS (
    SELECT
        id,
        account_type,
        opened_date,
        status,
        account_type_status,
        resolved_status,
        opened_date_minus_26d,
        status_clean,
        segment,
        city,
        AVG(balance) OVER (PARTITION BY status, city) AS status_avg_balance,
        CUME_DIST() OVER (ORDER BY balance) AS balance_cume_dist,
        PERCENT_RANK() OVER w_ordered AS balance_percent_rank,
        AVG(balance) OVER w_part AS customer_id_avg_balance,
        SUM(balance) OVER w_part AS customer_id_sum_balance
    FROM kept
    WINDOW w_ordered AS (PARTITION BY city, status ORDER BY balance DESC NULLS LAST), w_part AS (PARTITION BY customer_id)
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'CREDIT'
        )
)
SELECT
    CASE WHEN customer_id_sum_balance >= 407089 THEN 'HIGH' ELSE 'LOW' END AS customer_id_sum_balance_tier,
    COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END) AS credit_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM eligible
GROUP BY customer_id_sum_balance_tier
ORDER BY credit_count DESC NULLS FIRST
