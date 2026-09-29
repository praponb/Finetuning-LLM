SELECT monthly_fee, 'subscription' AS source FROM subscriptions WHERE YEAR(start_date) >= 2024 UNION ALL SELECT revenue, 'store_sale' AS source FROM store_sales WHERE YEAR(sale_date) = 2024
