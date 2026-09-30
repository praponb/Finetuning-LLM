with filtered as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        revenue - units as revenue_per_unit
    from store_sales
    where sale_date >= date '2024-04-30'
      and units < 32
)
select *
from (
    select
        id,
        product_id,
        revenue_per_unit,
        revenue
    from filtered
) src
unpivot include nulls (
    value for metric in (revenue_per_unit as REVENUE_PER_UNIT, revenue as REVENUE_VAL)
)
