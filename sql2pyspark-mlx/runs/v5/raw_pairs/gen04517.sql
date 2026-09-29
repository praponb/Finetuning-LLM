select 'customer' as origin, city from customers where credit_limit < 11200 union select 'store' as origin, city from stores where store_name like '%Old%' order by city desc;
