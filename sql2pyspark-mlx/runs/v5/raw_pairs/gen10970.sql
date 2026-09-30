select * from (select month(paid_at) as paid_month, sum(amount) as total_amount from payments group by month(paid_at)) as t where total_amount > 6500;
