with filtered as (
    select
        id,
        city,
        region,
        opened_date,
        sqft,
        upper(store_name) as store_name_upper
    from stores
    where opened_date >= date '2023-05-20'
      and region is not null
),
matched_keys as (
    select city
    from filtered
    except
    select city
    from stores
    where store_name like '%Old%'
),
kept_rows as (
    select s.*
    from filtered s
    join matched_keys k on s.city = k.city
),
scored as (
    select
        *,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share,
        max(sqft) over (partition by city) as city_max_sqft,
        cume_dist() over w_running as sqft_cume_dist
    from kept_rows
    window w_running as (partition by region, city order by sqft desc nulls first)
),
eligible as (
    select s.*
    from scored s
    where exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.last_restock >= s.opened_date
        )
),
kept_keys as (
    select city, id
    from eligible
    intersect
    select city, id
    from stores
    where city <> 'New York'
),
kept_rows_stores as (
    select s.*
    from eligible s
    join kept_keys k on s.city = k.city and s.id = k.id
),
daily as (
    select
        region,
        max(sqft) as max_sqft,
        sort_array(collect_set(region)) as region_list
    from kept_rows_stores
    group by region
)
select
    region,
    max_sqft,
    region_list
from daily
order by max_sqft desc nulls last;
