select
    case when account_type in ('CHECKING', 'SAVINGS') then 'tier_1' else 'other' end as account_type_group,
    count(balance) as balance_count,
    concat_ws(', ', sort_array(collect_set(status))) as status_values,
    round(avg(balance), 2) as avg_balance
from accounts
where account_type != 'LOAN'
group by case when account_type in ('CHECKING', 'SAVINGS') then 'tier_1' else 'other' end
order by balance_count desc, account_type_group desc
limit 3
