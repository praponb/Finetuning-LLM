select a.id, a.balance, c.city
from accounts a
inner join customers c on a.customer_id = c.id
where c.city not in ('Bangkok', 'Chiang Mai')
