SELECT
    country,
    signup_month,
    consumer_count,
    row_count
FROM (
    SELECT
        country,
        MONTH(signup_date) AS signup_month,
        COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
        COUNT(*) AS row_count
    FROM customers
    WHERE city NOT IN ('Chiang Mai', 'Bangkok', 'London')
    GROUP BY country, MONTH(signup_date)
) AS agg
WHERE consumer_count >= 6
ORDER BY row_count ASC, consumer_count DESC
