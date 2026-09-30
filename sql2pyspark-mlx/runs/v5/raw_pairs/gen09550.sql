select c.id, count(s.id) subscription_count, max(s.monthly_fee) max_monthly_fee
from customers c
left outer join subscriptions s on c.id = s.customer_id
group by c.id;
