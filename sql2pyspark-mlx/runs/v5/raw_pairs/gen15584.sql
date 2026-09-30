with metrics as (
    select
        *,
        row_number() over (partition by brand order by stock_qty desc, id) as row_seq,
        corr(price, cost) over (partition by supplier_id, category) as corr_price_cost,
        last_value(stock_qty) ignore nulls over (partition by brand order by price, id rows between unbounded preceding and current row) as last_known_stock_qty
    from products
    where cost <= 180
),
filtered as (
    select
        *,
        abs(corr_price_cost) as abs_corr_price_cost,
        price - stock_qty as price_minus_stock_qty
    from metrics
    where corr_price_cost > 0.5
      and supplier_id is not null
),
not_flagged as (
    select s.*
    from filtered s
    where not exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.order_id in (18, 24, 25, 31)
        )
),
with_windows as (
    select
        *,
        sum(abs_corr_price_cost) over (partition by brand order by launch_date, id rows between unbounded preceding and current row) as running_sum_abs_corr_price_cost,
        max(abs_corr_price_cost) over (partition by brand order by launch_date, id rows between unbounded preceding and current row) as running_max_abs_corr_price_cost,
        count(abs_corr_price_cost) over (partition by brand order by launch_date, id rows between unbounded preceding and current row) as running_count_abs_corr_price_cost,
        min(abs_corr_price_cost) over (partition by brand order by launch_date, id rows between unbounded preceding and current row) as running_min_abs_corr_price_cost
    from not_flagged
),
enriched as (
    select
        a.*,
        p.supplier_name,
        p.rating,
        p.city
    from with_windows a
    left join suppliers p
        on a.supplier_id = p.id
),
not_flagged_products as (
    select s.*
    from enriched s
    where not exists (
            select 1
            from inventory ch
            where ch.product_id = s.id
              and ch.last_restock < s.launch_date
        )
),
stats as (
    select
        date_trunc('week', launch_date) as week_start,
        date_trunc('month', launch_date) as activity_month,
        category,
        count(*) as num_rows,
        bit_or(stock_qty) as stock_qty_bit_or
    from not_flagged_products
    group by date_trunc('week', launch_date), date_trunc('month', launch_date), category
    having count(*) < 1000
)
select
    week_start,
    activity_month,
    category,
    num_rows,
    stock_qty_bit_or
from stats
order by num_rows desc;
