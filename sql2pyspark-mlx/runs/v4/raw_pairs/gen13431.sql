WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(email, 'N/A') AS email_filled
    FROM customers
    WHERE signup_date >= TO_DATE('2023-06-13')
      AND city IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS seq_no,
        AVG(age) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS age_avg_2_prior,
        LEAD(credit_limit, 2) OVER (PARTITION BY country ORDER BY age, id) AS next_credit_limit
    FROM filtered
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY age_avg_2_prior DESC NULLS FIRST) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(age_avg_2_prior, 0) AS age_avg_2_prior_rounded
    FROM latest
    WHERE NOT is_active
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_amount,
        top1.discount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.amount AS order_amount, c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY country ORDER BY age_avg_2_prior_rounded, id) AS age_avg_2_prior_rounded_bucket,
        LAST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        NTILE(5) OVER (PARTITION BY segment ORDER BY discount, id) AS discount_bucket,
        ROUND(next_credit_limit / SUM(next_credit_limit) OVER (PARTITION BY city), 4) AS next_credit_limit_share,
        AVG(next_credit_limit) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_credit_limit_avg_6
    FROM with_last
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    seq_no,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    SUM(order_amount) FILTER (WHERE city = 'Tokyo') AS tokyo_total_order_amount,
    ROUND(AVG(discount_bucket), 2) AS avg_discount_bucket,
    ROUND(SUM(next_credit_limit_avg_6) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS next_credit_limit_avg_6_per_tokyo
FROM metrics_customers
GROUP BY DATE_TRUNC('MONTH', signup_date), seq_no
HAVING ROUND(SUM(next_credit_limit_avg_6) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) >= 0
