WITH cleaned AS (
    SELECT *, CONCAT(account_type, ' - ', status) account_type_status
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status IS DISTINCT FROM 'FROZEN'
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date ASC, id ASC) row_seq,
        SUM(balance) OVER (PARTITION BY customer_id, status ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance ASC, id ASC) row_seq_2
    FROM cleaned
    WHERE account_type IN ('CHECKING', 'CREDIT')
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY status, customer_id ORDER BY row_seq DESC, id ASC) row_seq_bucket,
        AVG(row_seq) OVER (PARTITION BY status, account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) row_seq_avg_2,
        AVG(row_seq) OVER (PARTITION BY status ORDER BY row_seq_2 ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) row_seq_avg_3
    FROM metrics
),
matched_keys AS (
    SELECT customer_id, id
    FROM scored
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM accounts
    WHERE balance < 69000
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    MAX(row_seq_avg_3) FILTER (WHERE status = 'FROZEN') AS frozen_max_row_seq_avg_3,
    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_rows,
    ROUND(SUM(row_seq_avg_3) / NULLIF(COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END), 0), 2) AS row_seq_avg_3_per_credit,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date) ORDER BY ROUND(SUM(row_seq_avg_3) / NULLIF(COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END), 0), 2) DESC) AS row_seq_avg_3_per_credit_percent_rank
FROM verified
GROUP BY 1, 2
