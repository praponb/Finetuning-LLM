with CustomerStats as (
    select customer_id, round((100 * sum(case when status = 'FROZEN' then 1 else 0 end)) / count(*), 1) pct_frozen
    from accounts
    where opened_date < '2023-10-01'
    group by customer_id
)
select
    c.id,
    c.is_active,
    c.segment,
    C.pct_frozen
from customers c
join CustomerStats C on c.id = C.customer_id
where C.pct_frozen >= 54
