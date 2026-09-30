select x.country, y.region from (select distinct country from customers) x cross join (select distinct region from stores) y order by country, region;
