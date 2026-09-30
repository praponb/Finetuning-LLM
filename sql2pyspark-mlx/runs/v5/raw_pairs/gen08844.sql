select id, city, credit_limit, ntile(4) over (partition by city order by credit_limit desc, id) as credit_limit_ntile from customers where credit_limit <= 6600;
