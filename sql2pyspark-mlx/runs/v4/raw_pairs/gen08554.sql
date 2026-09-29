select s.region, count(i.id) as inventory_count from stores s left outer join inventory i on s.id = i.store_id where i.quantity > 155 group by s.region having count(i.id) > 1 order by region;
