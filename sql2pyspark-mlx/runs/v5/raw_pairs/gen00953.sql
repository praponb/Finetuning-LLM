with filtered as (
    -- Keep recent, valid rows
    select *, concat_ws(',', promo_code, channel) as promo_code_channel_key
    from promotions
    where ends_on >= date '2023-10-05'
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(discount_pct) over (partition by promo_code order by starts_on, id rows between 3 preceding and current row) as last4_count_discount_pct,
        min(discount_pct) over (partition by promo_code order by starts_on, id rows between 3 preceding and current row) as last4_min_discount_pct,
        avg(discount_pct) over (partition by promo_code order by starts_on, id rows between 3 preceding and current row) as last4_avg_discount_pct,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share
    from filtered
    where channel in ('web', 'mobile', 'store')
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(discount_pct_share) over (partition by channel order by last4_min_discount_pct, id rows between 3 preceding and current row) as last4_count_discount_pct_share,
        max(discount_pct_share) over (partition by channel order by last4_min_discount_pct, id rows between 3 preceding and current row) as last4_max_discount_pct_share,
        min(discount_pct_share) over (partition by channel order by last4_min_discount_pct, id rows between 3 preceding and current row) as last4_min_discount_pct_share,
        avg(discount_pct_share) over (partition by channel order by last4_min_discount_pct, id rows between 3 preceding and current row) as last4_avg_discount_pct_share,
        row_number() over (partition by channel, promo_code order by discount_pct, id) as row_seq
    from scored
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by channel order by last4_count_discount_pct_share desc) as last4_count_discount_pct_share_rank,
        avg(row_seq) over (partition by promo_code, channel order by last4_avg_discount_pct_share, id rows between 3 preceding and current row) as row_seq_avg_3,
        row_number() over (partition by promo_code order by row_seq, id) as event_seq
    from metrics
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by promo_code order by starts_on desc, id) as latest_rank
        from enriched
    ) ranked
    where latest_rank = 1
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when event_seq >= 9 then 'LARGE' else 'SMALL' end as event_seq_tier,
        sum(event_seq) filter (where promo_code = 'SPRING10') as spring10_total_event_seq,
        avg(last4_min_discount_pct_share) filter (where promo_code = 'BUNDLE') as bundle_avg_last4_min_discount_pct_share
    from latest
    group by case when event_seq >= 9 then 'LARGE' else 'SMALL' end
)
select
    event_seq_tier,
    spring10_total_event_seq,
    bundle_avg_last4_min_discount_pct_share
from summary
order by event_seq_tier desc, bundle_avg_last4_min_discount_pct_share desc
