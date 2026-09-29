with rollup_totals as (
    -- Join a dimension and aggregate
    select
        d.city,
        count(*) as rows_total,
        avg(s.quantity) as avg_quantity,
        grouping_id() as grouping_level
    from inventory s
    join stores d on s.store_id = d.id
    group by cube (d.city)
)
select
    city,
    rows_total,
    avg_quantity,
    grouping_level
from rollup_totals;
