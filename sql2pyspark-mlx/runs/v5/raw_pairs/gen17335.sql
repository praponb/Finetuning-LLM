select * from (select carrier, status, cost from shipments) src pivot (sum(cost) for status in ('DELIVERED', 'RETURNED'));
