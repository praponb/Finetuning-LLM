with flattened as (
    select
        id,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_limits) x as val
),
base as (
    select
        *,
        round(val * 0.85, 2) as val_adjusted,
        last_day(effective_from) as effective_from_month_end
    from flattened
    where val <= 22
),
enriched as (
    select
        *,
        cume_dist() over (partition by currency order by val_adjusted desc) as val_adjusted_cume_dist,
        stddev_samp(val) over w_part as currency_stddev_samp_val,
        sum(val) over w_part as currency_sum_val
    from base
    window w_part as (partition by currency)
),
cleaned as (
    select
        id,
        effective_from,
        val,
        effective_from_month_end,
        val_adjusted_cume_dist,
        currency_stddev_samp_val,
        currency_sum_val,
        initcap(trim(currency)) as currency_title
    from enriched
    where effective_from > date '2024-09-25'
      and val >= 63
)
select
    date_trunc('year', effective_from_month_end) as year_start,
    date_trunc('month', effective_from) as activity_month,
    approx_percentile(val, 0.5) as val_p50,
    round(avg(val_adjusted_cume_dist), 2) as avg_val_adjusted_cume_dist,
    percentile_approx(val, array(0.25, 0.5, 0.75)) as val_quartiles
from cleaned
group by date_trunc('year', effective_from_month_end), date_trunc('month', effective_from)
having val_p50 < 0;
