with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        code
    from carts
    lateral view outer explode(coupon_codes) x as code
),
base as (
    -- Keep recent, valid rows
    select *, case when code = 'WELCOME5' then 1 else 0 end as is_welcome5
    from flattened
    where created_at >= to_date('2025-03-02')
      and code is not null
      and code = 'VIP'
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        count(is_welcome5) over (partition by code order by cast(created_at as bigint) range between 900 preceding and current row) as is_welcome5_count_15m,
        max(is_welcome5) over (partition by code order by cast(created_at as bigint) range between 7200 preceding and current row) as is_welcome5_max_2h,
        dense_rank() over (partition by code order by is_welcome5 desc nulls last) as is_welcome5_dense_rank
    from base
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        created_at,
        code,
        is_welcome5,
        is_welcome5_count_15m,
        is_welcome5_max_2h,
        is_welcome5_dense_rank,
        count(is_welcome5_dense_rank) over (partition by code) as code_count_is_welcome5_dense_rank,
        min(is_welcome5_dense_rank) over (partition by code) as code_min_is_welcome5_dense_rank,
        avg(is_welcome5) over (partition by code order by cast(created_at as bigint) range between 3600 preceding and current row) as is_welcome5_avg_1h,
        dense_rank() over (partition by code order by is_welcome5) as is_welcome5_dense_rank_2,
        cume_dist() over (order by is_welcome5_dense_rank nulls last) as is_welcome5_dense_rank_cume_dist
    from enriched
),
base_carts as (
    -- Basic cleansing and defaults
    select
        *,
        round(is_welcome5_avg_1h / is_welcome5_dense_rank_2, 2) as is_welcome5_avg_1h_per_is_welcome5_dense_rank_2,
        weekofyear(created_at) as created_at_weekofyear
    from with_windows
    where created_at < '2023-05-26'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        greatest(is_welcome5_count_15m, code_min_is_welcome5_dense_rank) as greatest_is_welcome5_count_15m_code_min_is_welcome5_dense_rank,
        cast(is_welcome5_max_2h as string) as is_welcome5_max_2h_num
    from base_carts
    where created_at > date '2025-03-01'
      and is_welcome5_count_15m between 9 and 11
),
remaining_keys as (
    -- Set difference on keys
    select id
    from valid
    except
    select id
    from carts
    where array_contains(coupon_codes, 'VIP')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join remaining_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', created_at) as hour_window,
    case when is_welcome5_max_2h >= 63 then 'LARGE' else 'SMALL' end as is_welcome5_max_2h_tier,
    round(sum(code_count_is_welcome5_dense_rank) / nullif(count(case when code = 'VIP' then 1 end), 0), 2) as code_count_is_welcome5_dense_rank_per_vip,
    max(created_at) as ended_at,
    count(*) as row_count
from verified
group by date_trunc('hour', created_at), case when is_welcome5_max_2h >= 63 then 'LARGE' else 'SMALL' end
