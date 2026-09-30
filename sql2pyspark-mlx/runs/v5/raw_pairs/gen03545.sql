with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('-', promo_code, channel) as promo_code_channel_key,
        datediff(ends_on, starts_on) as days_starts_on_to_ends_on
    from promotions
    where channel is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        last_value(days_starts_on_to_ends_on, true) over (order by discount_pct, id rows between unbounded preceding and current row) as last_known_days_starts_on_to_ends_on,
        ntile(3) over (order by days_starts_on_to_ends_on, id) as days_starts_on_to_ends_on_bucket
    from cleaned
),
with_windows_promotions as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by promo_code order by ends_on desc, id) as seq_no,
        round(discount_pct / sum(discount_pct) over (partition by channel), 4) as discount_pct_share,
        avg(days_starts_on_to_ends_on_bucket) over (order by discount_pct, id rows between 3 preceding and current row) as days_starts_on_to_ends_on_bucket_avg_3
    from with_windows
    where discount_pct > 0.21
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by promo_code, channel order by ends_on desc, id) as latest_rank
    from with_windows_promotions
    qualify latest_rank = 1
),
with_windows_promotions_2 as (
    -- Running and moving aggregates
    select
        *,
        lag(discount_pct) over (partition by channel, promo_code order by days_starts_on_to_ends_on_bucket_avg_3, id) as prev_discount_pct,
        lead(discount_pct) over (partition by channel, promo_code order by days_starts_on_to_ends_on_bucket_avg_3, id) as next_discount_pct,
        lag(discount_pct_share) over (partition by channel, promo_code order by days_starts_on_to_ends_on_bucket_avg_3, id) as prev_discount_pct_share,
        lead(discount_pct_share) over (partition by channel, promo_code order by days_starts_on_to_ends_on_bucket_avg_3, id) as next_discount_pct_share,
        row_number() over (partition by channel, promo_code order by days_starts_on_to_ends_on_bucket_avg_3, id) as seq_no_2,
        percent_rank() over (order by discount_pct desc) as discount_pct_percent_rank
    from deduped
)
select
    id,
    promo_code,
    last_known_days_starts_on_to_ends_on,
    days_starts_on_to_ends_on_bucket,
    seq_no,
    discount_pct_share,
    days_starts_on_to_ends_on_bucket_avg_3,
    latest_rank,
    prev_discount_pct,
    case when promo_code rlike '^(BUNDLE|VIP)$' then 'CORE' when promo_code rlike '^(SPRING10|WELCOME5)$' then 'GROUP_A' else 'EXTENDED' end as promo_code_zone
from with_windows_promotions_2
order by promo_code_zone nulls first
