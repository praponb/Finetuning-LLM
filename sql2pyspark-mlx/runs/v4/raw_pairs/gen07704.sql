select x.country, y.city from (select distinct country from customers) x cross join (select distinct city from suppliers) y order by country, city;
