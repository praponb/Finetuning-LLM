SELECT
    YEAR(signup_date) signup_year,
    SUM(age) total_age,
    COUNT(*) cnt,
    ROUND(SUM(age) * 100.0 / SUM(SUM(age)) OVER (), 2) AS pct_of_total
FROM customers
WHERE LENGTH(email) >= 12
GROUP BY YEAR(signup_date)
ORDER BY cnt, pct_of_total DESC, signup_year
LIMIT 50
