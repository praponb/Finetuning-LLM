select
    id,
    start_date,
    end_date,
    monthly_fee,
    round(monthly_fee * 1.07, 2) monthly_fee_scaled
from subscriptions
where monthly_fee < 240;
