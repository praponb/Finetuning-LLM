select x.city, y.region from (select distinct city from suppliers) x cross join (select distinct region from stores) y order by city, region;
