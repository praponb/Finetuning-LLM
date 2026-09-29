select status from orders union all select status from shipments where order_id = 39 order by status;
