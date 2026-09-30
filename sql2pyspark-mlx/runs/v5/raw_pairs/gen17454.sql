with cleaned as (
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        array_join(tags, '|') as tags_text,
        upper(brand) as brand_upper,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where launch_date >= to_date('2023-08-31')
      and supplier_id is not null
),
with_windows as (
    select
        id,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        brand_upper,
        row_number() over (partition by supplier_id order by launch_date desc, id) as event_seq,
        sum(stock_qty) over (partition by supplier_id order by cost, id rows between unbounded preceding and current row) as cumulative_stock_qty,
        sum(price) over (partition by brand order by price, id rows between unbounded preceding and current row) as cumulative_price,
        ntile(3) over (partition by brand order by cost desc, id) as cost_bucket,
        row_number() over (partition by supplier_id order by price desc, id) as row_seq
    from cleaned
    where price > 1200
),
scored as (
    select
        id,
        brand,
        price,
        cost,
        launch_date,
        brand_upper,
        cumulative_stock_qty,
        cumulative_price,
        row_seq,
        avg(cumulative_stock_qty) over (partition by brand order by cost_bucket, id rows between 5 preceding and 1 preceding) as cumulative_stock_qty_avg_5_prior,
        avg(cumulative_stock_qty) over (partition by brand order by cost_bucket, id rows between 2 preceding and 1 preceding) as cumulative_stock_qty_avg_2_prior,
        avg(price) over (partition by brand order by cost, id rows between 6 preceding and current row) as price_avg_6
    from with_windows
),
kept as (
    select s.*
    from scored s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.revenue > 3500
        )
),
filtered as (
    select
        *,
        least(cumulative_stock_qty, price_avg_6) as least_cumulative_stock_qty_price_avg_6,
        coalesce(brand, 'UNKNOWN') as resolved_brand
    from kept
    where brand is not null
)
select
    date_trunc('month', launch_date) as activity_month,
    count(*) filter (where brand = 'Acme') as acme_rows,
    approx_percentile(price_avg_6, array(0.25, 0.5, 0.75)) as price_avg_6_quartiles,
    grouping_id() as grouping_id
from filtered
group by rollup (date_trunc('month', launch_date))
having count(*) filter (where brand = 'Acme') > 1
order by acme_rows;
