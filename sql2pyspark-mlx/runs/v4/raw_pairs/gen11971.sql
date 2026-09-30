SELECT channel, order_cohort, cnt
FROM (
    SELECT channel, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, COUNT(*) AS cnt
    FROM orders
    GROUP BY channel, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS sub
WHERE cnt >= 7
ORDER BY cnt ASC, channel ASC, order_cohort ASC
LIMIT 5;
