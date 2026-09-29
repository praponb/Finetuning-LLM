select a.city, b.browser from (select distinct city from customers) a cross join (select distinct browser from sessions) b order by city, browser;
