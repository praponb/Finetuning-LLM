with valid as (
    -- Keep recent, valid rows
    select
        *,
        replace(supplier_name, ' ', '-') as supplier_name_slug,
        coalesce(city, 'UNKNOWN') as city_filled
    from suppliers
    where city in ('Berlin', 'Chiang Mai')
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(4) over (order by rating, id) as rating_bucket,
        ntile(4) over w_grp as rating_bucket_2,
        percent_rank() over (partition by city order by rating nulls last) as rating_percent_rank
    from valid
    window w_grp as (partition by city order by rating, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.brand = 'Umbrella'
        )
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.brand is null
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by country order by rating_bucket nulls first) as latest_rank
        from eligible
    ) x
    where latest_rank = 1
)
-- Grouped metrics
select
    case when rating >= 3.6 then 'HIGH' else 'LOW' end as rating_tier,
    count(case when city = 'Tokyo' then 1 end) as tokyo_count,
    sum(case when country = 'US' then rating_bucket else 0 end) as us_rating_bucket,
    sum(rating) filter (where country = 'US') as us_total_rating,
    sort_array(collect_set(city)) as city_list
from latest
group by cube (case when rating >= 3.6 then 'HIGH' else 'LOW' end)
order by rating_tier desc nulls first
