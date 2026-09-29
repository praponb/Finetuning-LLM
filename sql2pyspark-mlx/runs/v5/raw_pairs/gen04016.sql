select
    case when year(paid_at) < 2024 then 'before_2024' when year(paid_at) = 2024 then '2024' else 'after_2024' end paid_cohort,
    case when amount >= 1230 then 'high' when amount >= 700 then 'mid' else 'low' end amount_bucket,
    status,
    min(amount) min_amount,
    count(case when status = 'REFUNDED' then 1 end) refunded_count
from payments
group by case when year(paid_at) < 2024 then 'before_2024' when year(paid_at) = 2024 then '2024' else 'after_2024' end, case when amount >= 1230 then 'high' when amount >= 700 then 'mid' else 'low' end, status
