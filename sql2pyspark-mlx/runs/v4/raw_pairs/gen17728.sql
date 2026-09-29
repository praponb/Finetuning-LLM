select id, region, sqft, opened_date from (select *, row_number() over (partition by region order by sqft desc, id) as pos from stores) as ranked where pos <= 2
