with valid as (
    select
        *,
        price + cost margin,
        array_contains(tags, 'premium') is_premium
    from products
    where launch_date >= to_date('2023-08-23')
),
metrics as (
    select
        *,
        lead(margin, 1) over (partition by brand order by cost, id) next_margin,
        avg(price) over (partition by category, brand order by price, id rows between 5 preceding and current row) price_avg_5
    from valid
    where category not in ('Books', 'Electronics', 'Fashion')
),
latest_match as (
    select s.*, x.quantity
    from metrics s
    join lateral (
            select c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_windows as (
    select
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        next_margin,
        quantity,
        lead(price_avg_5) over (partition by category order by quantity, id) next_price_avg_5,
        lag(cost) over (partition by category order by quantity, id) prev_cost,
        row_number() over (partition by category order by quantity, id) seq_no,
        row_number() over (partition by brand order by price_avg_5, id) row_seq
    from latest_match
)
select
    dim.city,
    dim.country,
    a.brand,
    count(*) as num_rows,
    max(a.row_seq) as max_row_seq
from with_windows a
inner join suppliers dim on a.supplier_id = dim.id
group by rollup (dim.city, dim.country, a.brand)
order by country nulls first;
