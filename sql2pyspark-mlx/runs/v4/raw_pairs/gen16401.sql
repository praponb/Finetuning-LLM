select p.id, p.supplier_id, p.product_name
from products p
where p.stock_qty >= 234 and exists (select 1 from store_sales ss where ss.product_id = p.id);
