with base as (
    -- Basic cleansing and defaults
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        initcap(trim(supplier_name)) as supplier_name_title,
        coalesce(city, 'OTHER') as city_clean
    from suppliers
    where country = 'US'
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as running_count_rating,
        min(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as running_min_rating,
        avg(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as running_avg_rating
    from base
),
kept_keys as (
    -- Set intersection on keys
    select city, id
    from scored
    intersect
    select city, id
    from suppliers
    where city = 'Bangkok'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join kept_keys k on s.city = k.city and s.id = k.id
),
scored_suppliers as (
    -- Window metrics over several different partitions
    select
        *,
        round(rating / sum(rating) over (partition by country, city), 4) as rating_share,
        cume_dist() over w_part as running_avg_rating_cume_dist
    from reconciled
    where country = 'TH'
    window w_part as (partition by city order by running_avg_rating desc)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        round(running_count_rating / sum(running_count_rating) over (partition by country, city), 4) as running_count_rating_share,
        stddev_samp(rating) over w_hist as city_stddev_samp_rating,
        dense_rank() over (order by running_min_rating nulls first) as running_min_rating_dense_rank,
        rank() over (partition by country order by rating desc nulls first) as rating_rank,
        max(running_avg_rating_cume_dist) over w_recent as country_max_running_avg_rating_cume_dist
    from scored_suppliers
    window w_hist as (partition by city), w_recent as (partition by country)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.supplier_id = 28
        )
)
-- Grouped metrics
select
    case when rating_share >= 0.61 then 'LARGE' else 'SMALL' end as rating_share_tier,
    case when rating >= 3.8 then 'LARGE' else 'SMALL' end as rating_tier,
    approx_percentile(rating, array(0.25, 0.5, 0.75)) as rating_quartiles,
    min(running_min_rating_dense_rank) as min_running_min_rating_dense_rank,
    avg(running_avg_rating_cume_dist) filter (where city = 'Tokyo') as tokyo_avg_running_avg_rating_cume_dist,
    avg(rating_rank) as avg_rating_rank,
    count_if(country = 'US') as us_events,
    percentile_approx(running_avg_rating_cume_dist, 0.5) as running_avg_rating_cume_dist_p50,
    dense_rank() over (partition by case when rating_share >= 0.61 then 'LARGE' else 'SMALL' end order by avg(running_avg_rating_cume_dist) filter (where city = 'Tokyo') desc) as tokyo_avg_running_avg_rating_cume_dist_dense_rank
from kept
group by case when rating_share >= 0.61 then 'LARGE' else 'SMALL' end, case when rating >= 3.8 then 'LARGE' else 'SMALL' end
order by rating_tier nulls last;
