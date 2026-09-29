select ss.id, ss.store_id from store_sales ss where ss.store_id in (select id from stores where city in ('Berlin', 'Tokyo', 'Chiang Mai')) order by id desc limit 20
