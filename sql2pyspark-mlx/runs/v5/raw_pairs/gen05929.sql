select id, delivered_date, cost, lead(cost, 1) over (order by delivered_date, id) as next_cost from shipments
