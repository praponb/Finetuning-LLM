select
    txn_type,
    account_id,
    case when merchant in ('Starbucks', 'Grab') then 'priority' else 'other' end as merchant_group,
    round(avg(amount), 2) as avg_amount,
    max(amount) - min(amount) as amount_range
from transactions
group by txn_type, account_id, case when merchant in ('Starbucks', 'Grab') then 'priority' else 'other' end
order by account_id desc;
