WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(country, ', ', name) country_name
    FROM customers
    WHERE signup_date >= DATE '2023-09-22'
      AND segment IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        age - credit_limit age_minus_credit_limit
    FROM valid
    WHERE city IN ('Tokyo', 'Bangkok')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at >= s.signup_date
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        age_minus_credit_limit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY age_minus_credit_limit ASC NULLS FIRST, id ASC) AS row_num
        FROM eligible
    ) x
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SUM(age_minus_credit_limit) FILTER (WHERE country = 'JP') AS jp_total_age_minus_credit_limit,
    SUM(CASE WHEN segment = 'SMB' THEN age_minus_credit_limit ELSE 0 END) AS smb_age_minus_credit_limit,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY SUM(CASE WHEN segment = 'SMB' THEN age_minus_credit_limit ELSE 0 END) DESC) AS smb_age_minus_credit_limit_percent_rank
FROM deduped
GROUP BY DATE_TRUNC('MONTH', signup_date), DATE_TRUNC('QUARTER', signup_date)
