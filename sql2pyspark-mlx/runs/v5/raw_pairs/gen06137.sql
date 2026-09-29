with filtered as (
    select
        *,
        dayofweek(ends_on) ends_on_dayofweek,
        coalesce(channel, 'UNKNOWN') channel_clean
    from promotions
    where ends_on > to_date('2023-06-15')
      and promo_code in ('WELCOME5', 'SPRING10', 'BUNDLE')
),
with_windows as (
    select
        *,
        first_value(channel, true) over (partition by promo_code order by starts_on, id rows between unbounded preceding and current row) first_known_channel,
        avg(discount_pct) over (order by discount_pct, id rows between 5 preceding and 1 preceding) discount_pct_avg_5_prior
    from filtered
),
base as (
    select
        id,
        ends_on,
        discount_pct,
        ends_on_dayofweek,
        channel_clean,
        first_known_channel,
        discount_pct_avg_5_prior,
        dayofweek(starts_on) starts_on_dayofweek,
        coalesce(channel, 'NONE') resolved_channel
    from with_windows
    where channel is distinct from 'partner'
      and ends_on >= date '2023-06-27'
),
remaining_keys as (
    select id
    from base
    intersect all
    select id
    from promotions
    where ends_on > '2023-04-01'
),
reconciled as (
    select s.*
    from base s
    join remaining_keys k on s.id = k.id
),
scored as (
    select *, rank() over (order by starts_on_dayofweek nulls last) starts_on_dayofweek_rank
    from reconciled
    where starts_on_dayofweek = 40
)
select
    id,
    ends_on,
    discount_pct,
    ends_on_dayofweek,
    first_known_channel,
    starts_on_dayofweek,
    resolved_channel
from scored
order by id desc nulls last;
