SELECT status, QUARTER(opened_date) AS opened_quarter, MIN(opened_date) AS first_opened_date FROM accounts GROUP BY ROLLUP(status, QUARTER(opened_date)) ORDER BY first_opened_date;
