with lines as (
    select
        id,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_limits) li as val
),
with_windows as (
    select
        *,
        dense_rank() over (partition by currency order by val asc) as val_dense_rank,
        cume_dist() over (partition by currency order by val asc nulls first) as val_cume_dist,
        count(val) over (partition by currency) as currency_count_val,
        avg(val) over (partition by currency) as currency_avg_val,
        count(val) over (partition by currency) as currency_count_val_2
    from lines
),
matched_keys as (
    select currency
    from with_windows
    intersect
    select currency
    from price_tiers
    where effective_from between '2025-06-01' and '2025-10-05'
),
verified as (
    select s.*
    from with_windows s
    inner join matched_keys k on s.currency = k.currency
)
select
    date_trunc('quarter', effective_from) as quarter_start,
    round(sum(currency_avg_val) / nullif(count(case when currency = 'EUR' then 1 end), 0), 2) as currency_avg_val_per_eur,
    percentile_approx(val_cume_dist, 0.5) as val_cume_dist_p50,
    sum(case when currency = 'EUR' then currency_count_val else 0 end) as eur_currency_count_val,
    percentile_approx(val_cume_dist, 0.5) as val_cume_dist_p50_2
from verified
group by date_trunc('quarter', effective_from)
order by val_cume_dist_p50_2 desc nulls first
