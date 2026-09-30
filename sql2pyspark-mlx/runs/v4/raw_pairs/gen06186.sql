select carrier, status, max(cost) - min(cost) as cost_range, count(distinct cost) as unique_orders from shipments where carrier = 'Kerry' group by carrier, status order by status;
