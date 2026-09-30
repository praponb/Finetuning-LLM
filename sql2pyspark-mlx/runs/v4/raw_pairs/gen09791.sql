select
    quarter(c.signup_date) as signup_quarter,
    case when c.country in ('TH', 'SG') then 'priority' else 'other' end as country_group,
    max(o.discount) as max_discount,
    min(o.amount) as min_amount
from orders o
left join customers c on c.id = o.customer_id
inner join stores s on o.store_id = s.id
group by quarter(c.signup_date), case when c.country in ('TH', 'SG') then 'priority' else 'other' end
order by 3
