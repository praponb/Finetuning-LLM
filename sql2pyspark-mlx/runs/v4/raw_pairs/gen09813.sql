select date_trunc('MONTH', paid_at) as paid_month, count(amount) as amount_count from payments group by 1 order by paid_month desc
