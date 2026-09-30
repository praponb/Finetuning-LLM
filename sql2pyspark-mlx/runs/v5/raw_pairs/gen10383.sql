with scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        discount,
        avg(shipping_fee) over (partition by channel order by order_date, id rows between 5 preceding and current row) as shipping_fee_avg_5,
        corr(discount, shipping_fee) over (partition by customer_id) as corr_discount_shipping_fee,
        last_value(shipping_fee, true) over (partition by status order by discount, id rows between unbounded preceding and current row) as last_known_shipping_fee,
        cume_dist() over w_recent as discount_cume_dist
    from orders
    window w_recent as (partition by store_id order by discount desc nulls first)
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from scored a
    left join promotions pr
        on coalesce(a.channel, 'store') = pr.channel
       and a.order_date >= pr.starts_on
       and a.order_date < pr.ends_on + interval 30 days
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        b.age,
        b.signup_date
    from attributed a
    left join customers b
        on a.customer_id <=> b.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched
    qualify rank() over (partition by customer_id order by promo_id desc) = 1
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from deduped a
    left join promotions pr
        on a.channel = pr.channel
       and a.signup_date >= pr.starts_on
       and a.signup_date <= date_add(pr.ends_on, 10)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        lead(promo_id) over w_running as next_promo_id,
        lag(promo_id) over w_running as prev_promo_id,
        lag(corr_discount_shipping_fee) over w_running as prev_corr_discount_shipping_fee,
        row_number() over w_running as event_seq,
        lag(discount, 1) over (partition by customer_id order by promotion_discount_pct, id, promo_id, promo_id_2) as prev_discount
    from matched
    window w_running as (partition by promo_code order by promo_id_2, id, promo_id, promo_id_2)
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, top1.quantity
    from with_windows s
    left join lateral (
            select c.quantity
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
),
stats as (
    -- Grouped metrics
    select
        case when age >= 63 then 'HIGH' else 'LOW' end as age_tier,
        round(sum(discount_cume_dist) / nullif(count(case when promotion_promo_code = 'VIP' then 1 end), 0), 2) as discount_cume_dist_per_vip,
        count(*) as event_count
    from latest_match
    group by case when age >= 63 then 'HIGH' else 'LOW' end
)
select age_tier
from stats
order by age_tier desc nulls first
