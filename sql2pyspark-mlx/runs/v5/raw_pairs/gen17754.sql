select
    case when amount >= 1500 then 'high' when amount >= 260 then 'mid' else 'low' end as amount_bucket,
    sort_array(collect_set(method)) as method_list,
    sum(case when status = 'FAILED' then amount else 0 end) as failed_amount,
    min(paid_at) as first_paid_at
from payments
where method = 'cash' and amount between 710 and 720
group by case when amount >= 1500 then 'high' when amount >= 260 then 'mid' else 'low' end
having failed_amount > 27;
