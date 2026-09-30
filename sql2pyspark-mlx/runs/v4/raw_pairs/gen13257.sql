select carrier, sum(cost) as total_cost, count(*) as cnt, round(sum(cost) * 100.0 / sum(sum(cost)) over (), 2) as pct_of_total from shipments where carrier = 'DHL' group by carrier;
