select year(paid_at) as paid_year, sum(amount) as total_amount, round(avg(amount), 2) as avg_amount
from payments
where amount >= 1550 and order_id is null and status = 'REFUNDED'
group by year(paid_at)
order by avg_amount desc;
