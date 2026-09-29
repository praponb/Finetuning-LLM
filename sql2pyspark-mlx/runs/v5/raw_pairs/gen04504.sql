with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        concat(store_name, ' - ', city) as store_name_city,
        coalesce(region, 'NONE') as region_clean
    from stores
    where opened_date > to_date('2023-04-26')
      and sqft >= 10862
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        store_name_city,
        region_clean,
        case when city = 'Bangkok' then 1 else 0 end as is_bangkok
    from filtered
    where region is not null
      and region in ('West', 'East', 'Central')
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.region,
        s.opened_date,
        s.region_clean,
        top1.quantity
    from cleaned s
    join lateral (
            select c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Ranking and gap features
    select
        *,
        avg(quantity) over (partition by region order by opened_date asc, id asc rows between unbounded preceding and current row) as running_avg_quantity,
        min(quantity) over (partition by region order by opened_date asc, id asc rows between unbounded preceding and current row) as running_min_quantity,
        count(quantity) over (partition by region order by opened_date asc, id asc rows between unbounded preceding and current row) as running_count_quantity,
        stddev_samp(quantity) over (partition by region) as region_stddev_samp_quantity
    from latest_match
)
-- Grouped metrics
select
    city,
    count(case when region = 'South' then 1 end) as south_count,
    round(sum(region_stddev_samp_quantity) / nullif(count(case when city = 'New York' then 1 end), 0), 2) as region_stddev_samp_quantity_per_new_york,
    count(case when region = 'Central' then 1 end) as central_count
from scored
group by city
