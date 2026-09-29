select p.channel, a.status as shipment_status, count(*) as rows_total, min(a.cost) as min_cost from shipments a inner join orders p on a.order_id = p.id group by p.channel, a.status
