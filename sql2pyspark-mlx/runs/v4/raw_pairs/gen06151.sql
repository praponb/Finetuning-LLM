with base as (
    -- Basic cleansing and defaults
    select *, add_months(last_restock, 6) as last_restock_plus_months
    from inventory
    where last_restock >= to_date('2023-03-05')
      and quantity is null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify rank() over (partition by store_id, product_id order by last_restock nulls last) = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(last_restock_plus_months) as last_restock_plus_months_month_end
    from deduped
    where last_restock <= '2025-08-01'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_plus_months,
        last_restock_plus_months_month_end,
        percent_rank() over (order by quantity nulls last) as quantity_percent_rank,
        avg(quantity) over (partition by store_id) as store_id_avg_quantity
    from valid
),
summary as (
    -- Aggregate with conditional metrics
    select
        product_id,
        case when quantity_percent_rank >= 0.84 then 'PREMIUM' else 'STANDARD' end as quantity_percent_rank_tier,
        date_trunc('year', last_restock) as year_start,
        count(distinct id) as distinct_ids,
        count(distinct product_id) as distinct_product_ids,
        sum(quantity) as total_quantity
    from with_windows
    group by product_id, case when quantity_percent_rank >= 0.84 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', last_restock)
)
select
    s.product_id,
    s.quantity_percent_rank_tier,
    s.year_start,
    s.distinct_ids,
    s.distinct_product_ids,
    s.total_quantity,
    (select min(t2.units) from store_sales t2 where t2.product_id = s.product_id) as min_store_sale_units
from summary s;
