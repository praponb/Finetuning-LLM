WITH base AS (
    SELECT *, CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE opened_date >= DATE '2023-07-07'
      AND customer_id IS NOT NULL
)
SELECT
    customer_id,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    account_type,
    COUNT(*) FILTER (WHERE account_type = 'CREDIT') AS credit_rows,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count,
    APPROX_PERCENTILE(balance_int, ARRAY(0.25, 0.5, 0.75)) AS balance_int_quartiles,
    GROUPING(customer_id) AS grouping_id
FROM base
GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('MONTH', opened_date), account_type), (customer_id, DATE_TRUNC('MONTH', opened_date)), (customer_id))
ORDER BY credit_rows DESC NULLS LAST;
