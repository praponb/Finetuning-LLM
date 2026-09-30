with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        rec.qty,
        rec.price,
        rec.promo
    from carts
    lateral view outer explode(items) t as rec
),
lines_carts as (
    select
        customer_id,
        created_at,
        promo,
        value
    from lines
    lateral view explode(coupon_codes) t as value
),
cleaned as (
    select
        *,
        case when value = 'WELCOME5' then 1 else 0 end as is_welcome5,
        coalesce(promo, 'N/A') as promo_clean
    from lines_carts
    where promo is not distinct from 'SPRING10'
      and created_at < '2024-02-01'
),
enriched as (
    select
        *,
        stddev_samp(is_welcome5) over (partition by customer_id, promo) as customer_id_stddev_samp_is_welcome5,
        rank() over (partition by customer_id order by is_welcome5 desc nulls first) as is_welcome5_rank
    from cleaned
),
cleaned_carts as (
    select
        customer_id,
        created_at,
        promo,
        value,
        is_welcome5,
        customer_id_stddev_samp_is_welcome5,
        lower(value) as value_lower,
        coalesce(value, 'NONE') as value_clean
    from enriched
    where value is not null
),
enriched_carts as (
    select *, count(customer_id_stddev_samp_is_welcome5) over (partition by promo order by cast(created_at as bigint) asc range between 7200 preceding and current row) as customer_id_stddev_samp_is_welcome5_count_2h
    from cleaned_carts
),
daily as (
    select
        case when customer_id_stddev_samp_is_welcome5 >= 63 then 'LARGE' else 'SMALL' end as customer_id_stddev_samp_is_welcome5_tier,
        date_trunc('hour', created_at) as hour_window,
        avg(is_welcome5) filter (where value = 'WELCOME5') as welcome5_avg_is_welcome5,
        size(collect_set(value)) as num_values,
        count_if(promo = 'WELCOME5') as welcome5_events
    from enriched_carts
    group by case when customer_id_stddev_samp_is_welcome5 >= 63 then 'LARGE' else 'SMALL' end, date_trunc('hour', created_at)
    having welcome5_events > 1
)
select
    customer_id_stddev_samp_is_welcome5_tier,
    hour_window,
    welcome5_avg_is_welcome5
from daily
order by hour_window desc, welcome5_avg_is_welcome5 desc
