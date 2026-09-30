SELECT product_id, SUM(quantity) total_quantity, ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total
FROM order_items
GROUP BY product_id;
