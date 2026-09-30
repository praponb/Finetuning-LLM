with filtered as (
    select
        *,
        last_day(starts_on) as starts_on_month_end,
        coalesce(channel, 'DIRECT') as resolved_channel
    from promotions
    where starts_on > date '2023-04-22'
      and channel is not null
),
with_windows as (
    select
        id,
        channel,
        discount_pct,
        resolved_channel,
        avg(discount_pct) over (partition by promo_code order by discount_pct, id rows between 3 preceding and 1 preceding) as discount_pct_avg_3_prior,
        first_value(channel) ignore nulls over w_hist as first_known_channel,
        first_value(channel, true) over (partition by promo_code order by starts_on, id rows between unbounded preceding and current row) as first_known_channel_2,
        row_number() over (partition by promo_code order by ends_on, id) as event_seq,
        avg(discount_pct) over w_part as discount_pct_avg_2_prior
    from filtered
    where discount_pct >= 0.21
    window w_hist as (partition by channel order by starts_on, id rows between unbounded preceding and current row), w_part as (partition by promo_code order by starts_on, id rows between 2 preceding and 1 preceding)
),
cleaned as (
    select
        id,
        discount_pct,
        resolved_channel,
        first_known_channel,
        first_known_channel_2,
        event_seq,
        round(event_seq, 0) as event_seq_rounded,
        coalesce(channel, 'N/A') as channel_clean
    from with_windows
    where discount_pct < 0.23
),
metrics as (
    select
        id,
        resolved_channel,
        first_known_channel_2,
        event_seq,
        event_seq_rounded,
        row_number() over w_ordered as row_seq,
        last_value(event_seq_rounded) ignore nulls over (order by event_seq, id rows between unbounded preceding and current row) as last_known_event_seq_rounded
    from cleaned
    window w_ordered as (order by event_seq_rounded, id)
)
select
    id,
    resolved_channel,
    first_known_channel_2
from metrics
