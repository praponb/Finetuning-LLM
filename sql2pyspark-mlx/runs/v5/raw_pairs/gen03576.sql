select price as value from products union all select cost as value from shipments where carrier not in ('Kerry', 'FedEx', 'Flash')
