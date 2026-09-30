with scored as (
    select
        *,
        rank() over (order by sqft desc nulls last) as sqft_rank,
        lag(sqft, 2, 0) over (partition by region, city order by sqft, id) as prev_sqft,
        last_value(region) ignore nulls over (partition by city, region order by opened_date, id rows between unbounded preceding and current row) as last_known_region,
        last_value(region) ignore nulls over (partition by city order by opened_date, id rows between unbounded preceding and current row) as last_known_region_2
    from stores
),
base as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_rank,
        prev_sqft,
        round(sqft_rank * 100, 2) as sqft_rank_scaled,
        concat_ws('-', store_name, region) as store_name_region_key
    from scored
    where opened_date > date '2023-08-09'
      and region is not null
),
kept as (
    select s.*
    from base s
    where exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock >= s.opened_date
        )
)
select
    id,
    store_name,
    region,
    opened_date,
    sqft_rank,
    prev_sqft,
    case when store_name rlike '^(Mall Central|Old Town)$' then 'CORE' when store_name rlike '^(Siam Flagship|Downtown Express)$' then 'GROUP_A' else 'PRIORITY' end as store_name_zone
from kept
