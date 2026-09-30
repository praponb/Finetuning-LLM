with product_summary as (
    select product_id, min(sale_date) as min_sale_date
    from store_sales
    group by product_id
)
select p.id, p.brand, ps.min_sale_date
from products p
left outer join product_summary ps on p.id = ps.product_id
order by ps.min_sale_date;
