select order_id, count(amount) amount_count, sum(case when method = 'cash' then amount else 0 end) cash_amount from payments group by 1 order by 1
