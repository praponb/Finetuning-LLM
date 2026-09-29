with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        round(revenue / units, 2) as revenue_per_unit,
        round(revenue / units, 2) as revenue_per_unit_2
    from store_sales
    where sale_date > date '2023-05-15'
),
cleaned_store_sales as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue_per_unit,
        revenue_per_unit_2,
        datediff(current_date(), sale_date) as days_since_sale_date
    from cleaned
    where sale_date >= to_date('2023-06-11')
      and revenue <= 3500
),
scored as (
    -- Running and moving aggregates
    select
        id,
        product_id,
        revenue_per_unit,
        revenue_per_unit_2,
        days_since_sale_date,
        sum(days_since_sale_date) over (partition by store_id order by revenue_per_unit, id rows between unbounded preceding and current row) as running_days_since_sale_date,
        avg(days_since_sale_date) over (partition by product_id order by revenue_per_unit, id rows between 2 preceding and 1 preceding) as days_since_sale_date_avg_2_prior
    from cleaned_store_sales
),
metrics as (
    -- Ranking and gap features
    select
        id,
        product_id,
        revenue_per_unit,
        revenue_per_unit_2,
        days_since_sale_date,
        running_days_since_sale_date,
        lag(days_since_sale_date) over (partition by product_id order by revenue_per_unit, id) as prev_days_since_sale_date,
        lag(days_since_sale_date_avg_2_prior) over (partition by product_id order by revenue_per_unit, id) as prev_days_since_sale_date_avg_2_prior,
        lead(days_since_sale_date_avg_2_prior) over (partition by product_id order by revenue_per_unit, id) as next_days_since_sale_date_avg_2_prior,
        row_number() over (partition by product_id order by revenue_per_unit, id) as event_seq
    from scored
    where revenue_per_unit_2 between 51 and 74
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        revenue_per_unit,
        revenue_per_unit_2,
        days_since_sale_date,
        prev_days_since_sale_date_avg_2_prior,
        next_days_since_sale_date_avg_2_prior,
        event_seq,
        round(running_days_since_sale_date, 1) as running_days_since_sale_date_rounded
    from metrics
    where running_days_since_sale_date between 231 and 288
),
enriched as (
    -- Attach products attributes
    select
        a.id,
        a.product_id,
        a.revenue_per_unit,
        a.prev_days_since_sale_date_avg_2_prior,
        a.next_days_since_sale_date_avg_2_prior,
        p.stock_qty,
        p.price
    from filtered a
    left join products p
        on a.product_id = p.id
),
with_dims as (
    -- Attach products attributes
    select
        src.id,
        src.product_id,
        src.prev_days_since_sale_date_avg_2_prior,
        b.stock_qty as product_stock_qty
    from enriched src
    left join products b
        on src.product_id = b.id
)
-- Multi-level aggregation
select
    product_id,
    case when prev_days_since_sale_date_avg_2_prior >= 89 then 'LARGE' else 'SMALL' end as prev_days_since_sale_date_avg_2_prior_tier,
    count(distinct product_id) as distinct_product_ids,
    approx_percentile(product_stock_qty, 0.5) as product_stock_qty_p50,
    round(avg(prev_days_since_sale_date_avg_2_prior), 2) as avg_prev_days_since_sale_date_avg_2_prior,
    min(product_stock_qty) as min_product_stock_qty
from with_dims
group by product_id, case when prev_days_since_sale_date_avg_2_prior >= 89 then 'LARGE' else 'SMALL' end
order by distinct_product_ids nulls first;
