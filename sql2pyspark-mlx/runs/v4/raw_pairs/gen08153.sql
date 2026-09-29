select count(case when city = 'London' then 1 end) as london_count from customers;
