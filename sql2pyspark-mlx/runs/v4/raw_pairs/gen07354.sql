with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        case when discount_pct >= 0.36 then 'High' when discount_pct >= 0.17 then 'Medium' else 'Low' end as discount_pct_band,
        coalesce(channel, 'DIRECT') as channel_clean
    from promotions
    where ends_on >= date '2024-01-17'
      and channel is not null
      and promo_code = 'BUNDLE'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        promo_code,
        starts_on,
        channel_clean,
        upper(promo_code) as promo_code_upper,
        coalesce(channel, 'DIRECT') as channel_filled
    from base
    where starts_on >= date '2023-10-12'
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        starts_on,
        channel_clean,
        channel_filled,
        last_day(starts_on) as starts_on_month_end,
        add_months(starts_on, 1) as starts_on_plus_months
    from cleaned
    where starts_on > date '2023-06-21'
      and promo_code in ('SPRING10', 'WELCOME5', 'VIP')
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        row_number() over w_running as event_seq,
        row_number() over w_running as event_seq_2
    from filtered
    window w_running as (order by starts_on_month_end desc, id asc)
)
-- Aggregate with conditional metrics
select
    date_trunc('month', starts_on_plus_months) as activity_month,
    date_trunc('week', starts_on_month_end) as week_start,
    case when event_seq_2 >= 9 then 'HIGH' else 'LOW' end as event_seq_2_tier,
    round(avg(event_seq), 2) as avg_event_seq,
    percentile_approx(event_seq, 0.5) as event_seq_p50,
    round(avg(event_seq_2), 2) as avg_event_seq_2,
    max(event_seq) as max_event_seq,
    count(*) as num_rows
from with_windows
group by date_trunc('month', starts_on_plus_months), date_trunc('week', starts_on_month_end), case when event_seq_2 >= 9 then 'HIGH' else 'LOW' end;
