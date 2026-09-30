select ss.id, ss.product_id
from store_sales ss
where ss.store_id not in (select id from stores where opened_date >= '2024-08-02');
