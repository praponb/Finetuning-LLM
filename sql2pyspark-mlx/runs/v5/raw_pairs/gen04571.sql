select status, count(discount) as discount_count, sum(case when status = 'PENDING' then 1 else 0 end) as pending_count from orders where discount > 0.19 group by 1;
