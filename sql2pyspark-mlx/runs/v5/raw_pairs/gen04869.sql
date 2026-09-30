select ss.id, ss.product_id, ss.units from store_sales ss where ss.store_id not in (select id from stores where opened_date between '2023-08-01' and '2024-07-01') order by ss.product_id desc, id;
