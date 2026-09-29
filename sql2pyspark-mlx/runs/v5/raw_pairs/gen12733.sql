select
    a.id,
    a.status,
    a.account_type,
    a.opened_date,
    c.age,
    initcap(trim(a.account_type)) account_type_title
from accounts a
left join customers c on a.customer_id = c.id
where a.account_type not in ('CREDIT', 'CHECKING', 'LOAN') or c.name like '%Sri%'
order by c.age asc;
