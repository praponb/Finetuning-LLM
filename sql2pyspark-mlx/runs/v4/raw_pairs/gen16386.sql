select sum(case when carrier = 'FedEx' then cost else 0 end) fedex_cost, max(cost) - min(cost) cost_range from shipments;
