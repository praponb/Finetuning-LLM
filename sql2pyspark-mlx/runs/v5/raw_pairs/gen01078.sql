select *, coalesce(sqft, 0) as sqft_filled from stores order by city desc;
