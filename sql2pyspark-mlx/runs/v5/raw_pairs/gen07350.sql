with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        upper(store_name) as store_name_upper
    from stores
    where opened_date between '2025-04-02' and '2025-10-16'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(sqft) over (partition by region order by opened_date asc, id asc rows between 5 preceding and 1 preceding) as sqft_avg_5_prior,
        lead(sqft, 1, 0) over (order by sqft asc, id asc) as next_sqft
    from valid
    where region = 'West'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        sqft_avg_5_prior - sqft as sqft_avg_5_prior_minus_sqft,
        coalesce(region, 'UNKNOWN') as region_filled
    from scored
    where region is distinct from 'Central'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date <= s.opened_date
        )
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from kept
    qualify rank() over (partition by region order by sqft_avg_5_prior desc nulls first) = 1
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by region order by sqft_avg_5_prior_minus_sqft desc nulls last) as dedup_rank
        from ranked
    ) t
    where dedup_rank = 1
)
-- Grouped metrics
select
    region,
    sum(case when region = 'West' then next_sqft else 0 end) as west_next_sqft,
    count(distinct region) as distinct_regions,
    approx_percentile(sqft_avg_5_prior, 0.9) as sqft_avg_5_prior_p90,
    count(*) as num_rows,
    count_if(region = 'South') as south_events,
    grouping(region) as grouping_level
from latest
group by cube (region)
having count_if(region = 'South') > 1
