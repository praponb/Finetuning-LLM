with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        case when region in ('Central', 'West') then 'Group A' else 'Other' end as region_group
    from stores
    where opened_date >= date '2023-05-01'
      and region is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by region order by sqft nulls first) as sqft_dense_rank,
        first_value(region) ignore nulls over (partition by city order by opened_date, id rows between unbounded preceding and current row) as first_known_region
    from filtered
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.sale_date < s.opened_date
        )
),
valid as (
    -- Keep recent, valid rows
    select *, initcap(trim(city)) as city_title
    from kept
    where region = 'Central'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(sqft_dense_rank, true) over (partition by city, region order by opened_date, id rows between unbounded preceding and current row) as first_known_sqft_dense_rank,
        row_number() over (order by sqft desc, id) as event_seq
    from valid
    where sqft <= 4686
)
select
    id,
    store_name,
    city,
    region,
    sqft,
    region_group,
    first_known_region,
    first_known_sqft_dense_rank,
    event_seq
from metrics
order by region desc nulls last;
