select status, sum(case when account_type = 'SAVINGS' then balance else 0 end) as savings_balance, concat_ws(', ', sort_array(collect_set(account_type))) as account_type_values
from accounts
group by status
having sum(case when account_type = 'SAVINGS' then balance else 0 end) >= 73;
