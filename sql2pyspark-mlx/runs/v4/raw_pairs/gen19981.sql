select * from (select carrier, status, cost from shipments) pivot (count(cost) for status in ('IN_TRANSIT', 'DELIVERED', 'LOST', 'RETURNED')) order by carrier;
