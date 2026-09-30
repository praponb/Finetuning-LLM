WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE opened_date > TO_DATE('2023-04-14')
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.name,
        d.credit_limit
    FROM base src
    LEFT JOIN customers d
        ON src.customer_id = d.id
    WHERE d.name IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        opened_date_plus_months,
        status_filled,
        credit_limit,
        SUM(balance) OVER w_recent AS cumulative_balance,
        SUM(credit_limit) OVER w_recent AS cumulative_credit_limit,
        NTILE(3) OVER (ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        COUNT(balance) OVER (PARTITION BY status) AS status_count_balance
    FROM joined
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(credit_limit_bucket) OVER w_part AS last7_max_credit_limit_bucket,
        AVG(credit_limit_bucket) OVER w_part AS last7_avg_credit_limit_bucket,
        MIN(credit_limit_bucket) OVER w_part AS last7_min_credit_limit_bucket
    FROM metrics
    WINDOW w_part AS (PARTITION BY customer_id, status ORDER BY opened_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date_plus_months) AS quarter_start,
    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_rows,
    MAX(status_count_balance) AS max_status_count_balance,
    MIN(last7_max_credit_limit_bucket) AS min_last7_max_credit_limit_bucket,
    PERCENTILE_APPROX(cumulative_balance, 0.5) AS cumulative_balance_p50,
    APPROX_PERCENTILE(cumulative_balance, 0.95) AS cumulative_balance_p95,
    ROUND(SUM(status_count_balance) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS status_count_balance_per_open
FROM enriched
GROUP BY quarter_start
ORDER BY max_status_count_balance NULLS LAST
