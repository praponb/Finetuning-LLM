with supplier_summary as (
    select supplier_id, sum(price) as total_price, round(stddev(stock_qty), 2) as stddev_stock_qty
    from products
    group by supplier_id
)
select
    suppliers.id,
    suppliers.country,
    suppliers.rating,
    supplier_summary.total_price,
    supplier_summary.stddev_stock_qty
from suppliers
left join supplier_summary on suppliers.id = supplier_summary.supplier_id
where supplier_summary.total_price > 5700
order by suppliers.id desc, supplier_summary.stddev_stock_qty;
