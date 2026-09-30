WITH cleaned AS (
    SELECT
        *,
        CONCAT(email, ' ', country) AS email_country,
        COALESCE(segment, 'DIRECT') AS resolved_segment
    FROM customers
    WHERE city IS NOT NULL
)
SELECT
    *,
    ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
    NTILE(3) OVER (PARTITION BY country, city ORDER BY age DESC, id) AS age_bucket,
    SUM(credit_limit) OVER (PARTITION BY segment, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
    LAG(credit_limit, 1) OVER (ORDER BY credit_limit, id) AS prev_credit_limit
FROM cleaned
