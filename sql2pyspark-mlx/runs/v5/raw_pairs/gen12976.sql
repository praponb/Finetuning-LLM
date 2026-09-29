with metrics as (
    select
        id,
        product_name,
        category,
        cost,
        stock_qty,
        supplier_id,
        round(price / sum(price) over (partition by brand), 4) as price_share,
        sum(cost) over (partition by category, brand order by launch_date, id rows between unbounded preceding and current row) as cumulative_cost
    from products
    where supplier_id in (19, 23, 25)
),
with_dims as (
    select
        s.id,
        s.cost,
        s.supplier_id,
        s.price_share,
        s.cumulative_cost,
        p.city
    from metrics s
    join suppliers p
        on s.supplier_id <=> p.id
),
not_flagged as (
    select s.*
    from with_dims s
    where not exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.unit_price <= 230
        )
),
ranked as (
    select
        id,
        supplier_id,
        price_share,
        cumulative_cost,
        city
    from not_flagged
    qualify row_number() over (partition by supplier_id order by cost desc nulls last, id) = 1
),
kept as (
    select s.*
    from ranked s
    where not exists (
            select 1
            from inventory x
            where x.product_id = s.id
              and x.store_id = 2
        )
)
select
    supplier_id,
    case when price_share >= 0.76 then 'LARGE' else 'SMALL' end as price_share_tier,
    round(avg(price_share), 2) as avg_price_share,
    approx_percentile(price_share, array(0.25, 0.5, 0.75)) as price_share_quartiles,
    count(case when city = 'Tokyo' then 1 end) as tokyo_count,
    avg(cumulative_cost) filter (where city = 'Berlin') as berlin_avg_cumulative_cost,
    max(cumulative_cost) filter (where city = 'Bangkok') as bangkok_max_cumulative_cost,
    sort_array(collect_set(city)) as city_list,
    dense_rank() over (partition by supplier_id order by avg(cumulative_cost) filter (where city = 'Berlin') desc) as berlin_avg_cumulative_cost_dense_rank
from kept
group by supplier_id, case when price_share >= 0.76 then 'LARGE' else 'SMALL' end
order by tokyo_count;
