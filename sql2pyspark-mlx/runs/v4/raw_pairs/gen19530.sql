with flattened as (
    select
        id,
        created_at,
        explode(coupon_codes) as tag
    from carts
),
valid as (
    select *, case when tag = 'WELCOME5' then 1 else 0 end as is_welcome5
    from flattened
    where created_at >= date '2025-03-01'
      and tag in ('BUNDLE', 'WELCOME5', 'SPRING10')
),
scored as (
    select
        id,
        created_at,
        tag,
        is_welcome5,
        rank() over (partition by tag order by is_welcome5 asc) as is_welcome5_rank,
        min(is_welcome5) over w_recent as tag_min_is_welcome5,
        min(is_welcome5) over w_recent as tag_min_is_welcome5_2,
        max(is_welcome5) over (partition by tag order by cast(created_at as bigint) asc range between 1800 preceding and current row) as is_welcome5_max_30m
    from valid
    window w_recent as (partition by tag)
),
filtered as (
    select
        id,
        created_at,
        tag,
        is_welcome5,
        is_welcome5_rank,
        tag_min_is_welcome5,
        tag_min_is_welcome5_2,
        is_welcome5_max_30m,
        is_welcome5 + is_welcome5_rank as is_welcome5_plus_is_welcome5_rank,
        coalesce(tag, 'UNKNOWN') as tag_clean
    from scored
    where created_at >= to_timestamp('2025-03-02 02:00:00')
      and tag is not null
),
metrics as (
    select
        id,
        created_at,
        tag,
        is_welcome5,
        is_welcome5_rank,
        tag_min_is_welcome5,
        tag_min_is_welcome5_2,
        is_welcome5_max_30m,
        is_welcome5_plus_is_welcome5_rank,
        tag_clean,
        avg(is_welcome5_rank) over (order by cast(created_at as bigint) asc range between 900 preceding and current row) as is_welcome5_rank_avg_15m,
        sum(is_welcome5_max_30m) over (partition by tag order by cast(created_at as bigint) asc range between 1800 preceding and current row) as is_welcome5_max_30m_sum_30m
    from filtered
),
scored_carts as (
    select
        id,
        created_at,
        tag,
        is_welcome5,
        is_welcome5_rank,
        tag_min_is_welcome5,
        tag_min_is_welcome5_2,
        is_welcome5_max_30m,
        is_welcome5_plus_is_welcome5_rank,
        tag_clean,
        is_welcome5_rank_avg_15m,
        is_welcome5_max_30m_sum_30m,
        dense_rank() over (order by is_welcome5_rank_avg_15m desc) as is_welcome5_rank_avg_15m_dense_rank,
        count(is_welcome5_max_30m_sum_30m) over (order by cast(created_at as bigint) asc range between 3600 preceding and current row) as is_welcome5_max_30m_sum_30m_count_1h
    from metrics
),
valid_carts as (
    select
        id,
        created_at,
        is_welcome5,
        tag_min_is_welcome5,
        is_welcome5_plus_is_welcome5_rank,
        is_welcome5_rank_avg_15m,
        is_welcome5_max_30m_sum_30m,
        is_welcome5_max_30m_sum_30m_count_1h,
        case when tag in ('WELCOME5', 'BUNDLE') then 'Core' else 'Other' end as tag_group,
        coalesce(tag, 'NONE') as resolved_tag
    from scored_carts
    where is_welcome5_plus_is_welcome5_rank < 27
)
select
    date_trunc('hour', created_at) as hour_window,
    date_trunc('day', created_at) as activity_day,
    approx_percentile(tag_min_is_welcome5, 0.9) as tag_min_is_welcome5_p90,
    min(created_at) as first_seen,
    percentile_approx(is_welcome5_rank_avg_15m, array(0.25, 0.5, 0.75)) as is_welcome5_rank_avg_15m_quartiles
from valid_carts
group by grouping sets ((date_trunc('hour', created_at), date_trunc('day', created_at)), (date_trunc('hour', created_at)), ())
having approx_percentile(tag_min_is_welcome5, 0.9) > 0
order by tag_min_is_welcome5_p90 desc nulls last
