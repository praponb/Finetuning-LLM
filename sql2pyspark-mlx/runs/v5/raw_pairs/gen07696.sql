with customer_stats as (
    select customer_id, round(avg(balance), 2) as avg_balance
    from accounts
    where status = 'OPEN'
    group by customer_id
)
select
    c.id,
    c.segment,
    c.is_active,
    cs.avg_balance
from customers c
inner join customer_stats cs on c.id = cs.customer_id
where cs.avg_balance > 57
order by c.id
