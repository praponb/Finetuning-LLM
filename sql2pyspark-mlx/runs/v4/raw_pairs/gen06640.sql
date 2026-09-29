select x.country, y.city from (select distinct country from suppliers) x cross join (select distinct city from stores) y;
