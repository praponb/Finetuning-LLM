with valid as (
    -- Keep recent, valid rows
    select
        *,
        date_add(opened_date, 30) as opened_date_plus_30d,
        initcap(trim(region)) as region_title
    from stores
    where opened_date >= to_date('2023-08-13')
      and region in ('Central', 'East', 'South')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_plus_30d,
        region_title,
        round(sqft * 0.85, 2) as sqft_scaled,
        substring(store_name, 1, 4) as store_name_prefix,
        coalesce(region, 'N/A') as region_filled
    from valid
    where region is not null
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.amount,
        x.discount
    from cleaned s
    left join lateral (
            select c.amount, c.discount
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) x on true
),
metrics as (
    -- Ranking and gap features
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        opened_date_plus_30d,
        region_title,
        sqft_scaled,
        region_filled,
        first_value(sqft_scaled, true) over (order by amount, id rows between unbounded preceding and current row) as first_known_sqft_scaled,
        last_value(sqft, true) over (partition by city order by amount, id rows between unbounded preceding and current row) as last_known_sqft
    from with_last
)
-- Aggregate with conditional metrics
select
    date_trunc('year', opened_date_plus_30d) as year_start,
    case when last_known_sqft >= 14426 then 'HIGH' else 'LOW' end as last_known_sqft_tier,
    count_if(region = 'West') as west_events,
    round(sum(last_known_sqft) / nullif(count(case when region = 'Central' then 1 end), 0), 2) as last_known_sqft_per_central,
    count(case when region = 'South' then 1 end) as south_count,
    count(*) as num_rows,
    sum(first_known_sqft_scaled) as total_first_known_sqft_scaled
from metrics
group by year_start, last_known_sqft_tier
