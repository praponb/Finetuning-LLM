with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        lower(store_name) store_name_lower,
        coalesce(region, 'OTHER') region_filled
    from stores
    where opened_date > date '2023-03-16'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        round(sqft / sum(sqft) over (partition by region), 4) sqft_share,
        count(sqft) over (partition by region) region_count_sqft
    from filtered
    where sqft >= 7938
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        s.region,
        s.opened_date,
        s.sqft,
        s.store_name_lower,
        s.region_filled,
        x.sale_date
    from with_windows s
    join lateral (
            select c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.last_restock >= s.opened_date
        )
)
-- Multi-level aggregation
select
    region,
    sort_array(collect_set(region)) as region_list,
    count_if(region = 'East') as east_events,
    count(*) as row_count,
    sum(case when region = 'Central' then sqft else 0 end) as central_sqft
from kept
group by region
order by central_sqft desc nulls first
