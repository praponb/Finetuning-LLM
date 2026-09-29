select signup_date, 'customer' source from customers where segment = 'Corporate' union all select delivered_date, 'shipment' source from shipments where cost <= 83 order by signup_date
