select id, city, region, opened_date, sqft, lag(sqft, 1) over (partition by city, region order by opened_date, id) prev_sqft from stores order by 4;
