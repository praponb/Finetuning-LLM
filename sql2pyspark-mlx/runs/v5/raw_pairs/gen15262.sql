with enriched as (
    -- Ranking and gap features
    select
        id,
        store_name,
        region,
        opened_date,
        lead(sqft) over w_running as next_sqft,
        lag(sqft) over w_running as prev_sqft,
        row_number() over w_running as row_seq
    from stores
    where opened_date < '2024-09-01'
    window w_running as (partition by city order by opened_date, id)
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.opened_date,
        s.next_sqft,
        latest.status,
        latest.shipping_fee
    from enriched s
    left join lateral (
            select c.status, c.shipping_fee
            from orders c
            where c.store_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(next_sqft) over w_ordered as next_sqft_avg_2_prior,
        last_value(shipping_fee, true) over (partition by region order by shipping_fee, id rows between unbounded preceding and current row) as last_known_shipping_fee,
        ntile(10) over (partition by region, status order by next_sqft desc, id) as next_sqft_bucket
    from with_latest
    window w_ordered as (partition by region order by opened_date, id rows between 2 preceding and 1 preceding)
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_windows s
    where exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date <= s.opened_date
        )
)
-- Grouped metrics
select
    region,
    date_trunc('week', opened_date) as week_start,
    sort_array(collect_set(region)) as region_list,
    avg(shipping_fee) filter (where region = 'West') as west_avg_shipping_fee,
    count(*) as cnt,
    round(avg(next_sqft_avg_2_prior), 2) as avg_next_sqft_avg_2_prior,
    grouping(region) as grouping_level
from not_flagged
group by grouping sets ((region, date_trunc('week', opened_date)), (region))
order by week_start desc
