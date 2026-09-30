select * from (select carrier, status, cost from shipments) pivot (count(cost) for status in ('RETURNED', 'DELIVERED', 'LOST')) order by carrier
