with top_stores as (select id, region, sqft, row_number() over (partition by region order by sqft desc, id) as rnk from stores) select id, region, sqft from top_stores where rnk <= 5 order by 1 desc
