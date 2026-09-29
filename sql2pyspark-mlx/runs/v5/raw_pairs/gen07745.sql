select cost from shipments where carrier in ('DHL', 'Flash', 'Kerry') union all select price as cost from products
