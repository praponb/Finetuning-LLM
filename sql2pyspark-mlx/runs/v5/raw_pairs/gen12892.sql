select id, opened_date, region, round(sqft, 2) as sqft_rounded from stores where (city = 'Chiang Mai' and opened_date >= '2025-08-17') or opened_date <= '2024-03-01' order by id limit 5
