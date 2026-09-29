select cost as value, 'shipment' as kind from shipments where carrier in ('UPS', 'DHL') union all select amount as value, 'transaction' as kind from transactions order by kind desc
