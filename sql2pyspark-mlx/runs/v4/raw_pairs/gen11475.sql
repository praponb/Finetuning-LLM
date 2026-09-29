with base as (
    -- Keep recent, valid rows
    select
        *,
        round(discount_pct, 2) as discount_pct_rounded,
        coalesce(channel, 'N/A') as resolved_channel
    from promotions
    where starts_on >= date '2023-10-07'
      and promo_code <> 'VIP'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by channel order by discount_pct_rounded desc nulls first, id) as latest_rank
        from base
    ) ranked
    where latest_rank = 1
),
matched_keys as (
    -- Set intersection on keys
    select channel
    from deduped
    intersect
    select channel
    from promotions
    where year(starts_on) >= 2025
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join matched_keys k on s.channel = k.channel
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_rounded,
        resolved_channel,
        latest_rank,
        min(discount_pct_rounded) over (partition by channel) as channel_min_discount_pct_rounded,
        min(discount_pct_rounded) over (partition by channel) as channel_min_discount_pct_rounded_2
    from reconciled
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        resolved_channel,
        channel_min_discount_pct_rounded,
        channel_min_discount_pct_rounded_2,
        round(channel_min_discount_pct_rounded * 1.15, 2) as adjusted_channel_min_discount_pct_rounded
    from with_windows
    where channel is not null
)
select
    id,
    starts_on,
    ends_on,
    resolved_channel,
    channel_min_discount_pct_rounded,
    adjusted_channel_min_discount_pct_rounded
from cleaned;
