select *, concat(carrier, ' ', status) as carrier_status from shipments where status in ('DELIVERED', 'IN_TRANSIT', 'LOST') and carrier in ('FedEx', 'UPS', 'Flash')
