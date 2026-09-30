select year(a.opened_date) opened_year, round(stddev(t.amount), 2) stddev_amount, min(t.amount) min_amount
from transactions t
inner join accounts a on a.id = t.account_id
inner join customers c on c.id = a.customer_id
group by year(a.opened_date)
