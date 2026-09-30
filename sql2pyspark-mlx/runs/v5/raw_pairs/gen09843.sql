select id, city, credit_limit, row_number() over (partition by city order by credit_limit, id) as row_num from customers where city = 'London';
