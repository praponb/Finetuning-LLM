select id, sqft from stores where opened_date <= '2025-03-01' and (city = 'London' or city in ('Berlin', 'Singapore'));
