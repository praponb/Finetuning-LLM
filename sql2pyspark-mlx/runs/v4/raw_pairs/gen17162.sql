select status, max(cost) max_cost from shipments where carrier <> 'Flash' group by status having max(cost) > 37
