select id, status, cost, round(cost / sum(cost) over (partition by status), 3) as cost_share from shipments;
