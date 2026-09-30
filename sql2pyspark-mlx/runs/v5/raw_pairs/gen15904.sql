with base as (
    select
        id,
        order_id,
        amount,
        paid_at,
        substring(status, 1, 4) status_prefix
    from payments
    where method is not null
      and amount between 1130 and 1180
),
cleaned as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status_prefix,
        coalesce(amount, 0) amount_filled,
        date_trunc('WEEK', paid_at) paid_at_period
    from base
    where paid_at >= timestamp '2023-09-03 20:00:00'
      and amount is not null
),
deduped as (
    select
        id,
        order_id,
        amount,
        paid_at,
        amount_filled
    from cleaned
    qualify row_number() over (partition by order_id order by amount asc nulls last, id asc) = 1
),
latest as (
    select *
    from deduped
    qualify row_number() over (partition by order_id order by paid_at asc nulls first, id asc) = 1
),
enriched as (
    select s.*, d.channel
    from latest s
    left join orders d
        on s.order_id <=> d.id
    where d.amount <= 1210
),
with_window as (
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from enriched a
    left join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.paid_at between pr.starts_on and pr.ends_on
),
deduped_payments as (
    select *
    from (
        select *, row_number() over (partition by order_id order by amount desc, id asc, promo_id asc) as latest_rank
        from with_window
    ) x
    where latest_rank = 1
),
summary as (
    select
        case when amount >= 1520 then 'LARGE' else 'SMALL' end as amount_tier,
        case when promo_id >= 8 then 'PREMIUM' else 'STANDARD' end as promo_id_tier,
        round(sum(amount) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as amount_per_store,
        percentile_approx(discount_pct, 0.9) as discount_pct_p90
    from deduped_payments
    group by case when amount >= 1520 then 'LARGE' else 'SMALL' end, case when promo_id >= 8 then 'PREMIUM' else 'STANDARD' end
)
select
    amount_tier,
    promo_id_tier,
    amount_per_store
from summary;
