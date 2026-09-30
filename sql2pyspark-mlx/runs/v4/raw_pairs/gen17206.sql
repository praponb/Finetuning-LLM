select
    c.city,
    c.country,
    max(s.cost) as max_cost,
    round(avg(s.cost), 2) as avg_cost
from shipments s
left join orders o on s.order_id = o.id
inner join customers c on c.id = o.customer_id
group by c.city, c.country;
