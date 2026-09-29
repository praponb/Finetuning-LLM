with eligible as (
    select s.*
    from stores s
    where not exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.last_restock < s.opened_date
        )
),
cleaned as (
    select
        *,
        upper(city) as city_upper,
        coalesce(region, 'DIRECT') as resolved_region
    from eligible
    where opened_date >= date '2023-07-10'
),
metrics as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        city_upper,
        resolved_region,
        avg(sqft) over (partition by city order by opened_date, id rows between 5 preceding and 1 preceding) as sqft_avg_5_prior,
        ntile(4) over (order by sqft desc, id) as sqft_bucket
    from cleaned
),
kept as (
    select s.*
    from metrics s
    where exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.units between 25 and 27
        )
),
daily as (
    select
        case when sqft_bucket >= 8 then 'HIGH' else 'LOW' end as sqft_bucket_tier,
        case when sqft_avg_5_prior >= 16164 then 'LARGE' else 'SMALL' end as sqft_avg_5_prior_tier,
        size(collect_set(region)) as num_regions,
        count(distinct id) as distinct_ids,
        count(case when region = 'North' then 1 end) as north_count
    from kept
    group by rollup (case when sqft_bucket >= 8 then 'HIGH' else 'LOW' end, case when sqft_avg_5_prior >= 16164 then 'LARGE' else 'SMALL' end)
    having distinct_ids < 1
)
select
    sqft_bucket_tier,
    sqft_avg_5_prior_tier,
    num_regions,
    distinct_ids,
    north_count
from daily
order by sqft_bucket_tier desc
