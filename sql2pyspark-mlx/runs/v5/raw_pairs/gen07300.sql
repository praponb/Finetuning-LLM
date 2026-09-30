SELECT
    city,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    COUNT(*) AS num_rows,
    BIT_AND(age) AS age_bit_and
FROM customers
GROUP BY 1, 2
HAVING COUNT(*) >= 1
