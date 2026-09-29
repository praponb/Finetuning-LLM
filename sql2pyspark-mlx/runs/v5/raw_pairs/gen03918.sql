select ss.id, ss.store_id, ss.units, ss.revenue from store_sales ss where ss.store_id in (select id from stores where city = 'London');
