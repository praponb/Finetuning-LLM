SELECT opened_date AS event_date, 'account' AS source FROM accounts WHERE opened_date > '2023-04-01' UNION SELECT sale_date AS event_date, 'store_sale' AS source FROM store_sales ORDER BY event_date
