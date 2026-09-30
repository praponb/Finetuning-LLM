SELECT *, COALESCE(sqft, 0) AS sqft_filled, CONCAT(region, ' - ', city) AS region_city FROM stores WHERE NOT ((sqft <= 12876 AND region = 'East') OR opened_date BETWEEN '2023-04-01' AND '2025-03-01')
