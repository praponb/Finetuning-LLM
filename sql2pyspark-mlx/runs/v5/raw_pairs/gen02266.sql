select date_format(paid_at, 'yyyy-MM') as paid_month, sum(case when method = 'paypal' then amount else 0 end) as paypal_amount
from payments
where amount between 650 and 1010
group by 1
having sum(case when method = 'paypal' then amount else 0 end) >= 30
order by paypal_amount, paid_month
limit 50;
