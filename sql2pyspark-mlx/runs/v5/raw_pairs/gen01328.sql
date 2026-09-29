with base as (
    select *, case when sqft >= 15464 then 'High' when sqft >= 4743 then 'Medium' else 'Low' end as sqft_band
    from stores
    where opened_date > date '2023-02-13'
      and sqft <> 13582
),
ranked as (
    select *, rank() over (partition by city, region order by sqft desc nulls last) as rank_in_group
    from base
    qualify rank_in_group = 1
),
kept_keys as (
    select id
    from ranked
    except
    select id
    from stores
    where store_name like '%Downtow%'
),
kept_rows as (
    select s.*
    from ranked s
    join kept_keys k on s.id = k.id
),
eligible as (
    select s.*
    from kept_rows s
    where exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.store_id in (1, 5)
        )
)
select
    id,
    store_name,
    region,
    sqft,
    sqft_band,
    case when store_name rlike '^(Old Town|Airport Kiosk)$' then 'PRIORITY' when store_name rlike '^(Siam Flagship|Downtown Express)$' then 'EXTENDED' else 'GROUP_B' end as store_name_zone
from eligible
order by sqft desc, region;
