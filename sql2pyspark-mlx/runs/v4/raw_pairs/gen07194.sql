select
    customer_id,
    count(*) filter (where account_type = 'SAVINGS') as savings_rows,
    count_if(account_type = 'CREDIT') as credit_events,
    count(case when status = 'FROZEN' then 1 end) as frozen_count
from accounts
group by customer_id;
