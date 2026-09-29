select city, avg(sqft) avg_sqft, sum(case when city = 'New York' then 1 else 0 end) new_york_count from stores where opened_date > '2025-08-01' group by city order by avg_sqft, city limit 10
