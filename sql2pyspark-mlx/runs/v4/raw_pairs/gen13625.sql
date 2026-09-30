select id, product_id, revenue
from (
    select *, rank() over (partition by product_id order by revenue desc) as pos
    from store_sales
    where sale_date < '2024-05-12'
) as r
where pos = 1;
