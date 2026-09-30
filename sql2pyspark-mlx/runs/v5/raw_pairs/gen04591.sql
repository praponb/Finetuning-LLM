select x.segment, y.level from (select distinct segment from customers) x cross join (select distinct level from employees) y order by segment, level;
