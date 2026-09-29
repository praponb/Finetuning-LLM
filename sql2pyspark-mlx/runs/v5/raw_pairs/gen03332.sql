with scored as (
    select
        *,
        last_value(channel) ignore nulls over (order by starts_on, id rows between unbounded preceding and current row) as last_known_channel,
        stddev_samp(discount_pct) over (partition by promo_code) as promo_code_stddev_samp_discount_pct,
        last_value(channel, true) over (partition by channel order by starts_on, id rows between unbounded preceding and current row) as last_known_channel_2,
        lead(discount_pct, 2) over (partition by promo_code order by starts_on, id) as next_discount_pct
    from promotions
),
cleaned as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_code_stddev_samp_discount_pct,
        last_known_channel_2,
        next_discount_pct,
        date_format(ends_on, 'MMM yyyy') as ends_on_label
    from scored
    where starts_on > date '2023-07-04'
),
base as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        last_known_channel_2,
        next_discount_pct,
        ends_on_label,
        datediff(current_date(), starts_on) as days_since_starts_on
    from cleaned
    where next_discount_pct >= 0.26
      and starts_on > date '2023-04-02'
),
valid as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        ends_on_label,
        days_since_starts_on,
        add_months(starts_on, 12) as starts_on_plus_months
    from base
    where ends_on > date '2023-08-11'
),
filtered as (
    select *, weekofyear(ends_on) as ends_on_weekofyear
    from valid
    where starts_on > date '2023-04-04'
      and promo_code = 'SPRING10'
)
select
    promo_code,
    count(*) as num_rows,
    count(distinct promo_code) as distinct_promo_codes,
    avg(days_since_starts_on) as avg_days_since_starts_on,
    percentile_approx(days_since_starts_on, 0.9) as days_since_starts_on_p90,
    sum(case when promo_code = 'WELCOME5' then ends_on_weekofyear else 0 end) as welcome5_ends_on_weekofyear,
    round(avg(days_since_starts_on), 2) as avg_days_since_starts_on_2
from filtered
group by promo_code
order by promo_code nulls last
