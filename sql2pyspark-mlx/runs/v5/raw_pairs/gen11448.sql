SELECT *, COALESCE(cost, 0) AS cost_filled FROM products WHERE YEAR(launch_date) = 2023 AND cost BETWEEN 250 AND 560
