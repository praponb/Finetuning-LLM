with ranked_stores as (select *, row_number() over (partition by city order by sqft desc, id) as pos from stores) select id, city, sqft from ranked_stores where pos <= 2;
