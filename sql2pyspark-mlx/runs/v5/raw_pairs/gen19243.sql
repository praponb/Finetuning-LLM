select x.segment, y.city from (select distinct segment from customers) x cross join (select distinct city from stores) y order by segment, city;
