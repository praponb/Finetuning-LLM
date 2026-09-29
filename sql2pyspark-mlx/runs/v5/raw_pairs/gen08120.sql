select status from orders where amount < 1190 intersect select status from shipments order by status
