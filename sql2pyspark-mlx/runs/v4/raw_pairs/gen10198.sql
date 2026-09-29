select stores.city, stores.region, sort_array(collect_set(orders.status)) as status_list from orders left outer join stores on orders.store_id = stores.id group by stores.city, stores.region;
