with valid as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ceil(revenue) as revenue_whole,
        coalesce(units, 0) as units_filled
    from store_sales
    where sale_date >= to_date('2023-04-06')
),
latest as (
    select
        id,
        store_id,
        units,
        revenue
    from valid
    qualify row_number() over (partition by store_id order by sale_date desc, id) = 1
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by store_id order by revenue desc nulls last, id) as rank_in_group
        from latest
    ) x
    where rank_in_group = 1
),
metrics as (
    select
        *,
        first_value(revenue) ignore nulls over (partition by store_id order by rank_in_group, id rows between unbounded preceding and current row) as first_known_revenue,
        ntile(3) over (partition by store_id order by revenue, id) as revenue_bucket
    from ranked
),
with_dims as (
    select
        s.id,
        s.store_id,
        s.revenue,
        s.rank_in_group,
        s.revenue_bucket,
        d.opened_date,
        d.store_name
    from metrics s
    left join stores d
        on s.store_id = d.id
)
select
    id,
    revenue,
    revenue_bucket,
    opened_date,
    store_name,
    case when store_name rlike '^(Downtown Express|Mall Central)$' then 'EXTENDED' when store_name rlike '^(Siam Flagship|Airport Kiosk)$' then 'GROUP_B' else 'OTHER_ZONE' end as store_name_zone
from with_dims
order by revenue_bucket desc nulls first;
