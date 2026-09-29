SELECT store_id, avg_units, last_sale_date
FROM (
    SELECT store_id, AVG(units) AS avg_units, MAX(sale_date) AS last_sale_date
    FROM store_sales
    GROUP BY store_id
) AS agg
WHERE avg_units > 33
