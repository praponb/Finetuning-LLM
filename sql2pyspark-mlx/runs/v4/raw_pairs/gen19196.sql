select quarter(paid_at) as paid_quarter, sum(case when method = 'card' then amount else 0 end) as card_amount from payments group by quarter(paid_at) order by paid_quarter desc limit 1
