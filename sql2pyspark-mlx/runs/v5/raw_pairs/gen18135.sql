with deduped as (
    select
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags
    from (
        select *, row_number() over (partition by supplier_id order by price desc, id asc) as latest_rank
        from products
    ) t
    where latest_rank = 1
)
select
    category,
    date_trunc('week', launch_date) as week_start,
    count(*) filter (where category = 'Sports') as sports_rows,
    max(price) filter (where brand = 'Acme') as acme_max_price,
    count(*) as row_count,
    size(collect_set(category)) as num_categories,
    bit_or(stock_qty) as stock_qty_bit_or,
    dense_rank() over (partition by category order by count(*) filter (where category = 'Sports') desc) as sports_rows_dense_rank
from deduped
group by category, date_trunc('week', launch_date)
order by sports_rows desc;
