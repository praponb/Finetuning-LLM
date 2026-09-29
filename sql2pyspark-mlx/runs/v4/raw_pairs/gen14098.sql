with store_sales_by_product as (
    select product_id, min(revenue) as min_revenue, avg(revenue) as avg_revenue
    from store_sales
    group by product_id
)
select
    p.id,
    p.product_name,
    p.supplier_id,
    ssbp.min_revenue,
    ssbp.avg_revenue
from products p
left join store_sales_by_product ssbp on p.id = ssbp.product_id
where ssbp.avg_revenue >= 3600;
