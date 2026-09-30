with base as (
    select *, date_sub(ends_on, 22) as ends_on_minus_22d
    from promotions
    where ends_on > date '2023-08-25'
),
attributed as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from base e
    left join promotions pr
        on coalesce(e.channel, 'web') = pr.channel
       and e.ends_on_minus_22d between pr.starts_on and pr.ends_on
),
metrics as (
    select
        *,
        sum(promotion_discount_pct) over (partition by promo_code order by ends_on asc, id asc, promo_id asc rows between unbounded preceding and current row) as running_promotion_discount_pct,
        avg(discount_pct) over (partition by promotion_promo_code order by promotion_discount_pct asc, id asc, promo_id asc rows between 5 preceding and current row) as discount_pct_avg_5,
        avg(promotion_discount_pct) over (partition by promo_code order by starts_on asc, id asc, promo_id asc rows between 6 preceding and 1 preceding) as promotion_discount_pct_avg_6_prior
    from attributed
    where promo_id != 6
),
remaining_keys as (
    select channel, id
    from metrics
    intersect all
    select channel, id
    from promotions
    where starts_on <= '2023-12-01'
),
verified as (
    select s.*
    from metrics s
    inner join remaining_keys k on s.channel = k.channel and s.id = k.id
)
select
    case when promotion_discount_pct_avg_6_prior >= 0 then 'PREMIUM' else 'STANDARD' end as promotion_discount_pct_avg_6_prior_tier,
    case when promotion_discount_pct >= 0.35 then 'PREMIUM' else 'STANDARD' end as promotion_discount_pct_tier,
    case when running_promotion_discount_pct >= 2 then 'PREMIUM' else 'STANDARD' end as running_promotion_discount_pct_tier,
    count_if(channel = 'store') as store_events,
    count(case when promotion_promo_code = 'WELCOME5' then 1 end) as welcome5_count
from verified
group by 1, 2, 3
having store_events < 1000
order by promotion_discount_pct_avg_6_prior_tier desc nulls last
