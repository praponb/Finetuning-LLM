select id, city, opened_date, sqft, lead(sqft, 1) over (partition by city order by opened_date, id) as next_sqft from stores where sqft <= 9366
