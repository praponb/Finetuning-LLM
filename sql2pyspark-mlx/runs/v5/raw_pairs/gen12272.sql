select x.country, y.device from (select distinct country from suppliers) x cross join (select distinct device from sessions) y;
