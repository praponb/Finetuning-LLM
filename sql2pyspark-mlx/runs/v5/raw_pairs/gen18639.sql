select cost as value from shipments intersect select unit_price as value from order_items order by value;
