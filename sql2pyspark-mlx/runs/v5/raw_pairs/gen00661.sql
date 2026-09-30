with valid as (
    select *, lower(region) as region_lower
    from stores
    where opened_date > to_date('2023-08-30')
      and region is not null
),
metrics as (
    select
        *,
        sum(sqft) over (partition by city, region order by sqft, id rows between unbounded preceding and current row) as running_sqft,
        sum(sqft) over (order by opened_date, id rows between unbounded preceding and current row) as cumulative_sqft,
        avg(sqft) over (order by sqft, id rows between 3 preceding and 1 preceding) as sqft_avg_3_prior,
        avg(sqft) over (partition by city order by sqft, id rows between 6 preceding and 1 preceding) as sqft_avg_6_prior
    from valid
    where upper(trim(store_name)) = 'AIRPORT KIOSK'
),
base as (
    select
        id,
        store_name,
        city,
        region,
        sqft,
        region_lower,
        running_sqft,
        sqft_avg_3_prior,
        sqft_avg_6_prior,
        trim(store_name) as clean_store_name
    from metrics
    where region is distinct from 'South'
      and opened_date > date '2023-04-11'
),
kept as (
    select s.*
    from base s
    where exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.store_id is null
        )
),
with_last as (
    select
        s.*,
        latest.quantity,
        latest.last_restock
    from kept s
    left join lateral (
            select c.quantity, c.last_restock
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) latest on true
),
cleaned as (
    select
        *,
        round(sqft_avg_3_prior * 0.9, 2) as sqft_avg_3_prior_scaled,
        coalesce(region, 'N/A') as resolved_region
    from with_last
    where sqft_avg_3_prior between 6083 and 12827
)
select
    region,
    city,
    sum(quantity) filter (where region = 'West') as west_total_quantity,
    sum(case when city = 'Singapore' then sqft_avg_3_prior else 0 end) as singapore_sqft_avg_3_prior,
    sum(case when city = 'Bangkok' then sqft else 0 end) as bangkok_sqft
from cleaned
group by region, city
order by bangkok_sqft nulls last
