WITH recent_products AS (
    SELECT *
    FROM products
    WHERE category != 'Books' AND brand IN ('Initech', 'Stark')
),
agg AS (
    SELECT brand, SUM(CASE WHEN brand = 'Umbrella' THEN price ELSE 0 END) AS umbrella_price
    FROM recent_products
    GROUP BY brand
)
SELECT *
FROM agg
ORDER BY umbrella_price DESC, brand
LIMIT 10
