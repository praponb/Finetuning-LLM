with base as (
    -- Basic cleansing and defaults
    select *, round(discount_pct * 1.07, 2) discount_pct_adjusted
    from promotions
    where discount_pct <= 0.15
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by promo_code order by ends_on, id) as row_num
        from base
    ) ranked
    where row_num = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_adjusted,
        row_num,
        sum(discount_pct_adjusted) over (partition by channel order by ends_on, id rows between unbounded preceding and current row) cumulative_discount_pct_adjusted,
        avg(discount_pct) over (partition by channel order by ends_on, id rows between 6 preceding and 1 preceding) discount_pct_avg_6_prior,
        row_number() over (partition by promo_code order by row_num, id) event_seq,
        percent_rank() over (partition by channel order by discount_pct_adjusted nulls last) discount_pct_adjusted_percent_rank,
        first_value(channel) ignore nulls over (order by discount_pct, id rows between unbounded preceding and current row) first_known_channel
    from ranked
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        round(discount_pct * 1.15, 2) discount_pct_scaled,
        coalesce(discount_pct_adjusted, 0) discount_pct_adjusted_filled
    from with_windows
    where ends_on >= date '2023-10-21'
      and channel is not null
)
select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    row_num,
    event_seq,
    discount_pct_adjusted_percent_rank,
    case when promo_code rlike '^(SPRING10|WELCOME5)$' then 'GROUP_B' else 'PRIORITY' end as promo_code_zone
from filtered;
