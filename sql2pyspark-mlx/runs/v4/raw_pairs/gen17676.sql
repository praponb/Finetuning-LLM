select store_id, count(distinct quantity) as unique_stores, max(quantity) as max_quantity from inventory group by store_id order by unique_stores desc
