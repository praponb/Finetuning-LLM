select quarter(s.opened_date) as opened_quarter, sum(case when o.status = 'SHIPPED' then 1 else 0 end) as shipped_count
from orders o
left join customers c on c.id = o.customer_id
left join stores s on o.store_id = s.id
where lower(trim(c.email)) = 'jack@startup.io'
group by quarter(s.opened_date)
