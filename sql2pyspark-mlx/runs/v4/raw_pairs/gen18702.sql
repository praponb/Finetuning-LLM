with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        country,
        city,
        rating,
        concat_ws('-', country, supplier_name) as country_supplier_name_key,
        coalesce(city, 'OTHER') as city_filled
    from suppliers
    where city <> 'London'
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.category <> 'Sports'
        )
),
not_flagged_suppliers as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.launch_date <= '2024-04-01'
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        city,
        rating,
        country_supplier_name_key,
        city_filled,
        sum(rating) over w_grp as running_rating,
        row_number() over (partition by country, city order by rating, id) as event_seq,
        ntile(3) over (partition by country, city order by rating desc, id) as rating_bucket,
        ntile(4) over w_recent as rating_bucket_2,
        ntile(3) over w_recent_2 as rating_bucket_3
    from not_flagged_suppliers
    window w_grp as (partition by country, city order by rating, id rows between unbounded preceding and current row), w_recent as (partition by city order by rating, id), w_recent_2 as (partition by country order by rating, id)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(rating_bucket) over (partition by country, city order by event_seq, id rows between unbounded preceding and current row) as cumulative_rating_bucket,
        percent_rank() over (partition by country order by rating_bucket nulls last) as rating_bucket_percent_rank,
        lag(running_rating, 2) over (partition by city order by rating_bucket_3, id) as prev_running_rating
    from enriched
),
scored_suppliers as (
    -- Running and moving aggregates
    select
        *,
        last_value(running_rating, true) over (partition by country, city order by cumulative_rating_bucket, id rows between unbounded preceding and current row) as last_known_running_rating,
        last_value(city, true) over (order by rating, id rows between unbounded preceding and current row) as last_known_city,
        sum(running_rating) over (order by rating_bucket_2, id rows between unbounded preceding and current row) as cumulative_running_rating,
        last_value(rating_bucket_3) ignore nulls over w_running as last_known_rating_bucket_3,
        percent_rank() over (order by rating desc) as rating_percent_rank
    from scored
    window w_running as (partition by country order by rating_bucket_3, id rows between unbounded preceding and current row)
),
scored_suppliers_2 as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by country order by rating_percent_rank desc, id) as seq_no,
        row_number() over (partition by city order by event_seq desc, id) as event_seq_2,
        ntile(3) over (partition by city order by rating_bucket_percent_rank, id) as rating_bucket_percent_rank_bucket
    from scored_suppliers
)
-- Aggregate with conditional metrics
select
    country,
    count(*) filter (where city = 'Singapore') as singapore_rows,
    count(*) as row_count,
    size(collect_set(city)) as num_cities,
    round(avg(event_seq), 2) as avg_event_seq
from scored_suppliers_2
group by cube (country)
having count(*) >= 1
order by avg_event_seq;
