with metrics as (
    -- Ranking and gap features
    select
        *,
        avg(balance) over (partition by account_type order by opened_date, id rows between 2 preceding and 1 preceding) as balance_avg_2_prior,
        avg(balance) over (partition by status) as status_avg_balance,
        ntile(10) over (order by balance, id) as balance_bucket
    from accounts
)
select
    s.id,
    s.customer_id,
    s.account_type,
    s.opened_date,
    s.status,
    s.balance_avg_2_prior,
    s.status_avg_balance,
    case when account_type rlike '^(LOAN|CHECKING)$' then 'GROUP_B' else 'OTHER_ZONE' end as account_type_zone,
    (select max(o2.discount) from orders o2 where o2.customer_id = s.customer_id) as max_order_discount
from metrics s
order by max_order_discount desc;
