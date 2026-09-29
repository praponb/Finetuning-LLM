with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        opened_date,
        sqft,
        last_day(opened_date) as opened_date_month_end
    from stores
    where opened_date > date '2023-06-18'
      and region is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_name,
        sum(sqft) over (partition by city order by opened_date_month_end, id rows between unbounded preceding and current row) as running_sqft,
        row_number() over (partition by city order by sqft, id) as event_seq,
        percent_rank() over (order by sqft desc) as sqft_percent_rank
    from cleaned
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        event_seq,
        sqft_percent_rank,
        round(running_sqft, 1) as running_sqft_rounded,
        substring(store_name, 1, 2) as store_name_prefix
    from enriched
    where event_seq >= 8
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.discount,
        x.shipping_fee
    from valid s
    left join lateral (
            select c.discount, c.shipping_fee
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) x on true
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when running_sqft_rounded >= 65 then 'HIGH' else 'LOW' end as running_sqft_rounded_tier,
        case when event_seq >= 8 then 'LARGE' else 'SMALL' end as event_seq_tier,
        case when sqft_percent_rank >= 0.68 then 'PREMIUM' else 'STANDARD' end as sqft_percent_rank_tier,
        max(shipping_fee) as max_shipping_fee,
        count(*) as event_count
    from with_last
    group by case when running_sqft_rounded >= 65 then 'HIGH' else 'LOW' end, case when event_seq >= 8 then 'LARGE' else 'SMALL' end, case when sqft_percent_rank >= 0.68 then 'PREMIUM' else 'STANDARD' end
    having max(shipping_fee) < 1000000
)
select
    running_sqft_rounded_tier,
    event_seq_tier,
    sqft_percent_rank_tier,
    max_shipping_fee
from totals
order by max_shipping_fee, running_sqft_rounded_tier;
