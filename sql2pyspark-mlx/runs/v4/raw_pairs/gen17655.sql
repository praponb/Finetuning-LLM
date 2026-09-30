select delivered_date, 'shipment' as source from shipments union select last_restock as delivered_date, 'inventory' as source from inventory order by delivered_date;
