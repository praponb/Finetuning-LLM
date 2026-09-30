with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        end_date,
        status,
        floor(monthly_fee) as monthly_fee_whole
    from subscriptions
    where monthly_fee > 120
)
select status, customer_id
from base
except all
select status, customer_id
from subscriptions
where plan not in ('enterprise', 'basic', 'premium');
