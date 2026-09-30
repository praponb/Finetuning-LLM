with with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.order_status,
        latest.shipping_fee
    from customers s
    join lateral (
            select c.status as order_status, c.shipping_fee
            from orders c
            where c.customer_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) latest on true
),
scored as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by city order by credit_limit desc nulls first) as credit_limit_rank,
        round(age / sum(age) over (partition by city, country), 4) as age_share,
        last_value(shipping_fee) ignore nulls over (partition by city order by age asc, id asc rows between unbounded preceding and current row) as last_known_shipping_fee,
        first_value(order_status, true) over w_recent as first_known_order_status
    from with_last
    where country = 'SG'
    window w_recent as (partition by country order by credit_limit asc, id asc rows between unbounded preceding and current row)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by segment, order_status order by age_share asc, id asc) = 1
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest s
    where not exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.end_date >= s.signup_date
        )
),
with_latest as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.country,
        s.is_active,
        s.shipping_fee,
        s.age_share,
        x.monthly_fee,
        x.status
    from eligible s
    left join lateral (
            select c.monthly_fee, c.status
            from subscriptions c
            where c.customer_id = s.id
            order by c.monthly_fee desc, c.id desc
            limit 1
        ) x on true
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when shipping_fee >= 37 then 'HIGH' else 'LOW' end as shipping_fee_tier,
        case when age_share >= 0.7 then 'PREMIUM' else 'STANDARD' end as age_share_tier,
        max(monthly_fee) filter (where status = 'ACTIVE') as active_max_monthly_fee,
        round(sum(shipping_fee) / nullif(count(case when country = 'DE' then 1 end), 0), 2) as shipping_fee_per_de,
        size(collect_set(country)) as num_countries,
        count(*) as num_rows,
        count(*) as row_count,
        count(case when country = 'DE' then 1 end) as de_count,
        grouping_id() as grouping_level
    from with_latest
    group by rollup (case when shipping_fee >= 37 then 'HIGH' else 'LOW' end, case when age_share >= 0.7 then 'PREMIUM' else 'STANDARD' end)
)
select
    shipping_fee_tier,
    age_share_tier,
    active_max_monthly_fee,
    row_count,
    de_count
from summary
order by shipping_fee_tier desc nulls last;
