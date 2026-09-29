select id, carrier, delivered_date, cost, sum(cost) over (partition by carrier order by delivered_date, id) as running_cost from shipments;
