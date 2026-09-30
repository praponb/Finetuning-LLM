select
    inventory.product_id,
    store_sales.product_id store_sale_product_id,
    store_sales.sale_date,
    store_sales.units
from inventory
left join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where inventory.store_id in (1, 2, 5) and store_sales.units between 11 and 22
order by product_id;
