select
    month(opened_date) as opened_month,
    case when account_type in ('LOAN', 'CREDIT') then 'group_a' else 'other' end as account_type_group,
    concat_ws(', ', sort_array(collect_set(status))) as status_values,
    round(stddev(balance), 2) as stddev_balance
from accounts
where customer_id is not null
group by month(opened_date), case when account_type in ('LOAN', 'CREDIT') then 'group_a' else 'other' end
order by account_type_group desc;
