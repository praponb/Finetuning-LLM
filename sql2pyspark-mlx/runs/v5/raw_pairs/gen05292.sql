select id, carrier, shipped_date, cost, ntile(4) over (partition by carrier order by shipped_date, id) as bucket, min(cost) over (partition by carrier) as carrier_min_cost from shipments;
