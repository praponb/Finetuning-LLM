select o.status, o.shipping_fee, s.store_name
from orders o
left join stores s on o.store_id = s.id
where o.status <> 'PENDING' and s.store_name like 'Do%'
order by status
