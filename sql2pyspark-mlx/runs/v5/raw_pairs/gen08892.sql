with dim_totals as (
    select
        dim.city,
        e.channel,
        count(*) as rows_total,
        max(e.discount) as max_discount
    from orders e
    inner join customers dim on e.customer_id = dim.id
    group by rollup (dim.city, e.channel)
)
select channel, max_discount
from dim_totals
order by channel asc nulls first
