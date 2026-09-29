select status, shipped_date, carrier, round(cost, 0) as cost_rounded, round(cost, 2) as cost_rounded_2 from shipments
