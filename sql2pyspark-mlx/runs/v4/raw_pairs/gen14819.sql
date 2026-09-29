with cleaned as (
    select *, coalesce(city, 'N/A') as city_filled
    from suppliers
    where city not in ('Berlin', 'Tokyo', 'London')
),
not_flagged as (
    select s.*
    from cleaned s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.stock_qty between 124 and 211
        )
),
scored as (
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        city_filled,
        percent_rank() over (partition by city order by rating nulls last) as rating_percent_rank,
        ntile(10) over w_grp as rating_bucket
    from not_flagged
    window w_grp as (partition by country order by rating, id)
),
enriched as (
    select
        *,
        cume_dist() over (partition by city order by rating_bucket desc nulls first) as rating_bucket_cume_dist,
        row_number() over (partition by city order by rating_percent_rank, id) as event_seq,
        last_value(rating_percent_rank) ignore nulls over (partition by country order by rating_percent_rank, id rows between unbounded preceding and current row) as last_known_rating_percent_rank
    from scored
),
with_windows as (
    select
        *,
        ntile(4) over (partition by city order by rating, id) as rating_bucket_2,
        percent_rank() over w_grp as rating_percent_rank_2,
        percent_rank() over (partition by city, country order by last_known_rating_percent_rank nulls first) as last_known_rating_percent_rank_percent_rank,
        sum(rating_bucket_cume_dist) over w_ordered as cumulative_rating_bucket_cume_dist
    from enriched
    window w_grp as (partition by city order by rating desc nulls last), w_ordered as (partition by city order by rating, id rows between unbounded preceding and current row)
),
latest as (
    select *
    from with_windows
    qualify row_number() over (partition by city, country order by last_known_rating_percent_rank_percent_rank, id) = 1
)
select
    case when rating_bucket_2 >= 8 then 'HIGH' else 'LOW' end as rating_bucket_2_tier,
    case when event_seq >= 9 then 'PREMIUM' else 'STANDARD' end as event_seq_tier,
    case when cumulative_rating_bucket_cume_dist >= 4 then 'LARGE' else 'SMALL' end as cumulative_rating_bucket_cume_dist_tier,
    count(*) filter (where city = 'New York') as new_york_rows,
    max(rating_bucket_cume_dist) as max_rating_bucket_cume_dist,
    min(rating_bucket_cume_dist) as min_rating_bucket_cume_dist,
    rank() over (partition by case when rating_bucket_2 >= 8 then 'HIGH' else 'LOW' end order by min(rating_bucket_cume_dist) desc) as min_rating_bucket_cume_dist_rank
from latest
group by rating_bucket_2_tier, event_seq_tier, cumulative_rating_bucket_cume_dist_tier
order by cumulative_rating_bucket_cume_dist_tier desc;
