select
    c.segment,
    src.status,
    count(*) as rows_total,
    avg(src.monthly_fee) as avg_monthly_fee,
    grouping_id() as grouping_level
from subscriptions src
left join customers c on src.customer_id = c.id
group by rollup (c.segment, src.status)
order by rows_total asc nulls first;
