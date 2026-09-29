select * from (select status, carrier, cost from shipments where carrier in ('Kerry', 'FedEx', 'UPS'))
pivot (avg(cost) for carrier in ('FedEx', 'UPS', 'Kerry'));
