select city, country, round((100 * sum(case when city = 'Berlin' then 1 else 0 end)) / count(*), 1) as pct_berlin, min(rating) as min_rating from suppliers group by city, country;
