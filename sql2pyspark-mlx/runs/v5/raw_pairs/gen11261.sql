with base as (
    -- Basic cleansing and defaults
    select
        *,
        round(sqft, 2) as sqft_rounded,
        case when city = 'Singapore' then 1 else 0 end as is_singapore
    from stores
    where opened_date >= to_date('2023-04-30')
      and region is distinct from 'Central'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        last_value(region) ignore nulls over w_hist as last_known_region,
        first_value(is_singapore) ignore nulls over (order by sqft, id rows between unbounded preceding and current row) as first_known_is_singapore,
        avg(sqft) over (partition by city order by sqft_rounded, id rows between 3 preceding and 1 preceding) as sqft_avg_3_prior
    from base
    window w_hist as (partition by region order by sqft, id rows between unbounded preceding and current row)
),
with_windows_stores as (
    -- Ranking and gap features
    select
        *,
        avg(is_singapore) over w_hist as is_singapore_avg_5,
        lead(sqft_avg_3_prior, 2) over w_recent as next_sqft_avg_3_prior
    from with_windows
    where first_known_is_singapore > 57
    window w_hist as (partition by region order by sqft, id rows between 5 preceding and current row), w_recent as (partition by region, city order by is_singapore, id)
),
matched_keys as (
    -- Set difference on keys
    select city
    from with_windows_stores
    except all
    select city
    from stores
    where city = 'Berlin'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows_stores s
    inner join matched_keys k on s.city = k.city
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept_rows s
    where exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date <= s.opened_date
        )
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    sqft_rounded,
    first_known_is_singapore,
    sqft_avg_3_prior
from kept
order by city desc nulls last, id;
