select a.city, b.level from (select distinct city from customers) a cross join (select distinct level from employees) b order by city, level;
