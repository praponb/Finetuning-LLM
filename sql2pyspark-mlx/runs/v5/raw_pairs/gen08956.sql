select
    ss.id,
    ss.units,
    s.city,
    s.region,
    p.brand,
    p.launch_date,
    case when s.city = 'Berlin' then 1 else 0 end as is_berlin
from store_sales ss
inner join stores s on ss.store_id = s.id
left join products p on p.id = ss.product_id
where s.opened_date <= '2025-08-01';
