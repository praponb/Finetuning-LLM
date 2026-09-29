select
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    rank() over (partition by status order by monthly_fee desc nulls last) as monthly_fee_rank,
    ntile(5) over (partition by status order by monthly_fee, id) as monthly_fee_bucket
from subscriptions;
