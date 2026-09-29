with valid as (
    select
        *,
        case when region = 'Central' then 1 else 0 end as is_central,
        concat_ws(',', region, store_name) as region_store_name_key,
        coalesce(region, 'OTHER') as region_clean
    from stores
    where opened_date >= date '2024-02-06'
      and region is not null
),
kept as (
    select s.*
    from valid s
    where exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.sale_date < s.opened_date
        )
),
not_flagged as (
    select s.*
    from kept s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.discount < 0.21
        )
)
select
    case when is_central >= 63 then 'HIGH' else 'LOW' end as is_central_tier,
    round(avg(is_central), 2) as avg_is_central,
    size(collect_set(region)) as num_regions,
    sort_array(collect_set(region)) as region_list,
    percentile_approx(is_central, 0.9) as is_central_p90,
    sort_array(collect_set(region)) as region_list_2
from not_flagged
group by case when is_central >= 63 then 'HIGH' else 'LOW' end
order by is_central_tier desc
