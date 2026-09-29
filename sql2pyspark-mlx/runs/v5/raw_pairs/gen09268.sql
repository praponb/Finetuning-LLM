select *, hour(paid_at) as paid_at_hour, coalesce(method, 'Unknown') as method_filled from payments where order_id in (4, 14, 39) or paid_at >= '2023-07-01' order by amount
