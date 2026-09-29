SELECT id, brand, price, ROUND(cost, 1) AS cost_rounded FROM products WHERE price < 570 OR brand NOT IN ('Umbrella', 'Initech', 'Acme')
