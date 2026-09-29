with customer_totals as (
    select customer_id, concat_ws(', ', sort_array(collect_set(country))) as country_values
    from sessions
    where browser = 'Firefox'
    group by customer_id
)
select
    c.id,
    c.age,
    c.is_active,
    ct.country_values
from customers c
left outer join customer_totals ct on c.id = ct.customer_id
