SELECT brand, SUM(price) AS total_price, ROUND(SUM(price) * 100.0 / SUM(SUM(price)) OVER (), 2) AS pct_of_total
FROM products
WHERE product_name IS NOT NULL
GROUP BY brand
ORDER BY total_price DESC
