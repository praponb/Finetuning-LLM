select * from (select date_trunc('MONTH', paid_at) as paid_month, count(*) as payment_count from payments group by date_trunc('MONTH', paid_at)) as x where payment_count > 8;
