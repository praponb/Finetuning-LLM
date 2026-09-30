select c.country, min(t.amount) as min_amount
from transactions t
join accounts a on a.id = t.account_id
left join customers c on a.customer_id = c.id
where a.balance < 69000
group by c.country
order by country
limit 1;
