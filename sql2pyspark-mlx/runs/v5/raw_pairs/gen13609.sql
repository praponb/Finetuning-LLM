select
    id,
    status,
    monthly_fee,
    percent_rank() over (partition by status order by monthly_fee desc) as monthly_fee_pct_rank
from subscriptions
where monthly_fee > 170;
