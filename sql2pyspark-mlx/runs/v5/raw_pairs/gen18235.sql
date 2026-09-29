with ranked_stores as (select id, region, sqft, row_number() over (partition by region order by sqft desc, id) as rn from stores) select * from ranked_stores where rn <= 3
