with filtered as (
    -- Keep recent, valid rows
    select *, date_format(starts_on, 'yyyy-MM') as starts_on_label
    from promotions
    where ends_on >= date '2023-08-22'
      and channel is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        ends_on,
        discount_pct,
        datediff(current_date(), starts_on) as days_since_starts_on
    from filtered
    where promo_code != 'SPRING10'
),
scored as (
    -- Ranking and gap features
    select
        id,
        channel,
        ends_on,
        discount_pct,
        days_since_starts_on,
        sum(days_since_starts_on) over (partition by channel order by days_since_starts_on, id rows between unbounded preceding and current row) as running_days_since_starts_on,
        ntile(4) over (order by discount_pct, id) as discount_pct_bucket,
        dense_rank() over (partition by channel order by days_since_starts_on desc nulls first) as days_since_starts_on_dense_rank,
        first_value(channel, true) over (partition by channel order by ends_on, id rows between unbounded preceding and current row) as first_known_channel,
        sum(discount_pct) over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as cumulative_discount_pct
    from base
),
stats as (
    -- Grouped metrics
    select
        case when days_since_starts_on_dense_rank >= 8 then 'LARGE' else 'SMALL' end as days_since_starts_on_dense_rank_tier,
        count(case when channel = 'store' then 1 end) as store_count,
        percentile_approx(days_since_starts_on_dense_rank, 0.95) as days_since_starts_on_dense_rank_p95,
        max(discount_pct) filter (where channel = 'web') as web_max_discount_pct
    from scored
    group by days_since_starts_on_dense_rank_tier
)
select
    days_since_starts_on_dense_rank_tier,
    store_count,
    days_since_starts_on_dense_rank_p95,
    web_max_discount_pct
from stats
