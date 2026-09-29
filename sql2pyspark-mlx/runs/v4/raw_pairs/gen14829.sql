SELECT
    QUARTER(start_date) AS start_quarter,
    MAX(end_date) AS latest_end_date,
    MIN(start_date) AS min_start_date,
    MIN(end_date) AS first_end_date
FROM subscriptions
WHERE MONTH(start_date) IN (1, 3, 8) AND customer_id = 24
GROUP BY QUARTER(start_date);
