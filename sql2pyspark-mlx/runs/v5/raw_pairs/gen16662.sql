select date_format(s.opened_date, 'yyyy-MM') as opened_month, s.region, max(i.quantity) - min(i.quantity) as quantity_range
from inventory i
inner join stores s on i.store_id = s.id
left join products p on i.product_id = p.id
where i.product_id in (8, 10)
group by date_format(s.opened_date, 'yyyy-MM'), s.region
order by quantity_range desc, opened_month asc
