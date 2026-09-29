with base as (
    select
        *,
        last_day(opened_date) as opened_date_month_end,
        length(city) as city_length
    from stores
    where sqft < 7889
      and opened_date > date '2023-06-25'
),
with_last as (
    select s.*, latest.amount
    from base s
    join lateral (
            select c.amount
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) latest on true
),
kept_keys as (
    select city
    from with_last
    except
    select city
    from stores
    where region <> 'West'
),
kept_rows as (
    select s.*
    from with_last s
    inner join kept_keys k on s.city = k.city
),
not_flagged as (
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.channel = 'store'
        )
),
remaining_keys as (
    select id
    from not_flagged
    except
    select id
    from stores
    where lower(trim(store_name)) = 'downtown express'
),
verified as (
    select s.*
    from not_flagged s
    inner join remaining_keys k on s.id = k.id
),
kept as (
    select s.*
    from verified s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.order_date < s.opened_date
        )
),
scored as (
    select
        *,
        min(city_length) over (partition by region) as region_min_city_length,
        cume_dist() over (partition by region order by city_length) as city_length_cume_dist,
        avg(city_length) over (partition by city) as city_avg_city_length
    from kept
    where region = 'West'
)
select
    date_trunc('quarter', opened_date_month_end) as quarter_start,
    count(case when region = 'Central' then 1 end) as central_count,
    count(case when city = 'Tokyo' then 1 end) as tokyo_count,
    count(distinct id) as distinct_ids,
    count(*) filter (where city = 'Berlin') as berlin_rows
from scored
group by date_trunc('quarter', opened_date_month_end);
