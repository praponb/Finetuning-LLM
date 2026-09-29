select * from (select carrier, status, cost from shipments) pivot (avg(cost) for status in ('RETURNED', 'LOST', 'IN_TRANSIT', 'DELIVERED')) order by carrier;
