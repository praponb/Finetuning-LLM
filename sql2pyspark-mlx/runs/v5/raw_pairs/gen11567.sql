SELECT
    YEAR(launch_date) launch_year,
    COUNT(cost) cost_count,
    COUNT(*) cnt,
    MIN(launch_date) first_launch_date
FROM products
GROUP BY launch_year
ORDER BY cnt DESC, launch_year
LIMIT 10;
