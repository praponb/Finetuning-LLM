with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        entry.qty,
        entry.price,
        entry.promo
    from carts
    lateral view outer explode(items) e as entry
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        p.credit_limit,
        p.city
    from exploded e
    left join customers p
        on e.customer_id = p.id
    where p.segment is not null
),
enriched_carts as (
    -- Running and moving aggregates
    select
        *,
        min(credit_limit) over (partition by promo) as promo_min_credit_limit,
        percent_rank() over (partition by promo order by credit_limit desc nulls last) as credit_limit_percent_rank,
        round(price / sum(price) over (partition by customer_id, city), 4) as price_share,
        round(price / sum(price) over (partition by promo), 4) as price_share_2,
        stddev_samp(credit_limit) over (partition by promo) as promo_stddev_samp_credit_limit
    from enriched
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        coupon_codes,
        qty,
        price,
        promo,
        city,
        promo_min_credit_limit,
        credit_limit_percent_rank,
        price_share_2,
        promo_stddev_samp_credit_limit,
        percent_rank() over (partition by promo order by qty desc nulls last) as qty_percent_rank,
        max(price_share) over (partition by city) as city_max_price_share,
        dense_rank() over (partition by promo, customer_id order by qty) as qty_dense_rank
    from enriched_carts
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when qty >= 5 then 'HIGH' else 'LOW' end as qty_tier,
        sum(case when promo = 'WELCOME5' then qty_percent_rank else 0 end) as welcome5_qty_percent_rank,
        round(avg(qty), 2) as avg_qty,
        round(avg(promo_min_credit_limit), 2) as avg_promo_min_credit_limit,
        grouping_id() as gid
    from scored
    group by cube (case when qty >= 5 then 'HIGH' else 'LOW' end)
    having welcome5_qty_percent_rank >= 0
)
select
    qty_tier,
    welcome5_qty_percent_rank,
    avg_qty,
    gid
from daily
order by gid desc nulls first;
