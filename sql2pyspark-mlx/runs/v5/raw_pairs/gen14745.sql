SELECT product_name, price, category, DAYOFWEEK(launch_date) AS launch_date_dayofweek FROM products WHERE brand IN ('Globex', 'Acme', 'Stark') AND (stock_qty < 136 OR brand IS NULL)
