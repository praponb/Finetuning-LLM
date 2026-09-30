with dim_totals as (
    select
        d.city,
        a.status,
        count(*) as rows_total,
        max(a.discount) as max_discount,
        grouping_id() as level_id
    from orders a
    join customers d on a.customer_id = d.id
    group by grouping sets ((d.city, a.status), (d.city), ())
)
select
    city,
    status,
    rows_total,
    max_discount,
    level_id
from dim_totals;
