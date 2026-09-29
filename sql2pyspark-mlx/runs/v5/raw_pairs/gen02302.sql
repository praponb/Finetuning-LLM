select
    case when balance >= 77000 then 'high' when balance >= 16000 then 'mid' else 'low' end as balance_bucket,
    account_type,
    status,
    sort_array(collect_set(status)) as status_list
from accounts
group by case when balance >= 77000 then 'high' when balance >= 16000 then 'mid' else 'low' end, account_type, status
order by 1, status;
