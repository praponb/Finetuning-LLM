with kept_keys as (
    select method, id
    from payments
    intersect all
    select method, id
    from payments
    where amount <= 680
),
reconciled as (
    select s.*
    from payments s
    inner join kept_keys k on s.method = k.method and s.id = k.id
)
select
    case when amount >= 1380 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    status,
    sort_array(collect_set(method)) as method_list,
    avg(amount) as avg_amount,
    grouping_id() as gid
from reconciled
group by rollup (case when amount >= 1380 then 'PREMIUM' else 'STANDARD' end, status)
