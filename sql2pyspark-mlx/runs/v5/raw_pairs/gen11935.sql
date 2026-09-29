SELECT 'transaction' AS source, amount FROM transactions UNION SELECT 'store_sale' AS source, revenue AS amount FROM store_sales WHERE YEAR(sale_date) >= 2025 ORDER BY amount DESC;
