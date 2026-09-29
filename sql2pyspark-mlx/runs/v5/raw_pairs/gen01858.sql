with joined as (
    -- Attach products attributes
    select
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        a.revenue,
        d.cost
    from store_sales a
    inner join products d
        on a.product_id = d.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by store_id, product_id order by sale_date desc nulls last, id) as rank_in_group
    from joined
    qualify rank_in_group = 1
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        revenue,
        cost,
        rank_in_group,
        weekofyear(sale_date) as sale_date_weekofyear
    from deduped
    where sale_date > date '2023-08-23'
      and revenue > 2500
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        revenue,
        cost,
        sale_date_weekofyear,
        revenue - sale_date_weekofyear as revenue_minus_sale_date_weekofyear
    from valid
    where cost < 580
)
-- Grouped metrics
select
    case when cost >= 600 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    count(distinct id) as distinct_ids,
    round(avg(sale_date_weekofyear), 2) as avg_sale_date_weekofyear
from filtered
group by case when cost >= 600 then 'PREMIUM' else 'STANDARD' end;
