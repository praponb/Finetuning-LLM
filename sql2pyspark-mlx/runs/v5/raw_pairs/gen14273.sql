select id, city, region, sqft from (select *, rank() over (partition by city, region order by sqft desc) as row_num from stores) ranked where row_num = 1;
