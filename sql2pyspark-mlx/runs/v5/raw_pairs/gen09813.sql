select o.shipping_fee, o.status, s.opened_date, coalesce(o.shipping_fee, 0) as shipping_fee_filled from orders o right join stores s on o.store_id = s.id order by shipping_fee asc;
