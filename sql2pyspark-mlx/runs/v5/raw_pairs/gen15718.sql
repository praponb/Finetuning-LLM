WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(email) AS email_length,
        COALESCE(segment, 'OTHER') AS segment_filled
    FROM customers
    WHERE signup_date >= DATE '2023-07-07'
      AND city IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city), 4) AS credit_limit_share,
        SUM(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        CORR(age, credit_limit) OVER (PARTITION BY segment, country) AS corr_age_credit_limit,
        LEAD(email_length, 1) OVER (PARTITION BY country, segment ORDER BY credit_limit, id) AS next_email_length
    FROM valid
),
valid_customers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        email_length,
        running_age,
        corr_age_credit_limit,
        next_email_length,
        CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END AS is_consumer
    FROM scored
    WHERE age BETWEEN 41 AND 57
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY corr_age_credit_limit, id) = 1
)
-- Grouped metrics
SELECT
    country,
    CASE WHEN corr_age_credit_limit >= 0.65 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_age_credit_limit_tier,
    COUNT(*) AS num_rows,
    ROUND(AVG(age), 2) AS avg_age
FROM deduped
GROUP BY CUBE (country, CASE WHEN corr_age_credit_limit >= 0.65 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY avg_age;
