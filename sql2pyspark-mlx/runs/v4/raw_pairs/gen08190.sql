SELECT region, COALESCE(sqft, 0) AS sqft_filled FROM stores WHERE sqft BETWEEN 6855 AND 11147 AND opened_date BETWEEN '2024-10-01' AND '2025-07-01';
