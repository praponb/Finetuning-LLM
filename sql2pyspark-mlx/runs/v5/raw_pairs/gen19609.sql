with latest as (
    select *, row_number() over (partition by region, city order by sqft desc, id) as latest_rank
    from stores
    qualify latest_rank = 1
),
ranked as (
    select
        id,
        city,
        region,
        opened_date,
        sqft,
        latest_rank
    from (
        select *, row_number() over (partition by city order by latest_rank desc, id) as row_num
        from latest
    ) ranked
    where row_num = 1
),
scored as (
    select
        id,
        city,
        opened_date,
        latest_rank,
        max(sqft) over (partition by region order by opened_date, id rows between unbounded preceding and current row) running_max_sqft,
        avg(sqft) over (partition by region order by opened_date, id rows between unbounded preceding and current row) running_avg_sqft,
        min(sqft) over (partition by region order by opened_date, id rows between unbounded preceding and current row) running_min_sqft,
        count(sqft) over (partition by region order by opened_date, id rows between unbounded preceding and current row) running_count_sqft
    from ranked
),
latest_stores as (
    select
        id,
        latest_rank,
        running_avg_sqft,
        running_count_sqft
    from scored
    qualify row_number() over (partition by city order by running_avg_sqft desc nulls last, id) = 1
),
kept_keys as (
    select id
    from latest_stores
    except all
    select id
    from stores
    where city not in ('Bangkok', 'Chiang Mai', 'London')
),
reconciled as (
    select s.*
    from latest_stores s
    inner join kept_keys k on s.id = k.id
),
totals as (
    select
        case when running_count_sqft >= 17 then 'PREMIUM' else 'STANDARD' end as running_count_sqft_tier,
        count(*) as row_count,
        count(*) as cnt,
        round(avg(running_count_sqft), 2) as avg_running_count_sqft,
        count(distinct id) as distinct_ids,
        count(distinct id) as distinct_ids_2
    from reconciled
    group by 1
    having count(*) >= 1
)
select
    running_count_sqft_tier,
    row_count,
    cnt,
    distinct_ids_2
from totals
order by cnt nulls last;
