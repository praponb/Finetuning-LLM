select x.city, y.device from (select distinct city from customers) x cross join (select distinct device from sessions) y order by city, device
