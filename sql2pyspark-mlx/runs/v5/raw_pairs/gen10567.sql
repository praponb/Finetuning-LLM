select id, carrier, cost, ntile(4) over (partition by carrier order by cost, id) as quartile from shipments
