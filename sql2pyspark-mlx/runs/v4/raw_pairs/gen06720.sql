WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        CASE WHEN country = 'TH' THEN 1 ELSE 0 END AS is_th
    FROM customers
    WHERE signup_date >= TO_DATE('2023-10-23')
      AND city IS NOT NULL
      AND LENGTH(email) < 9
),
cleaned_customers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        TRIM(name) AS clean_name,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
    FROM cleaned
    WHERE signup_date >= TO_DATE('2024-01-09')
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_status
    FROM cleaned_customers s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest_match
    QUALIFY RANK() OVER (PARTITION BY country, city ORDER BY signup_date DESC NULLS LAST) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        clean_name,
        email_domain,
        order_status,
        ROW_NUMBER() OVER (PARTITION BY country, order_status ORDER BY signup_date, id) AS seq_no,
        FIRST_VALUE(order_status, TRUE) OVER (PARTITION BY city, order_status ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_order_status,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        LAST_VALUE(order_status, TRUE) OVER (PARTITION BY order_status ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_status
    FROM deduped
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    CASE WHEN seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
    SUM(seq_no) AS total_seq_no,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    PERCENTILE_APPROX(seq_no, 0.5) AS seq_no_p50,
    GROUPING(DATE_TRUNC('MONTH', signup_date)) AS grouping_level
FROM metrics
GROUP BY ROLLUP (DATE_TRUNC('MONTH', signup_date), CASE WHEN seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING seq_no_p50 >= 0;
