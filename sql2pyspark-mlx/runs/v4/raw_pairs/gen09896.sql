select city, avg(sqft) avg_sqft, count(sqft) sqft_count from stores where opened_date <= '2023-09-01' group by city having count(sqft) > 3;
