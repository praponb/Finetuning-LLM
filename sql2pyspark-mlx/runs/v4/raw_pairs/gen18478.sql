with base as (
    -- Basic cleansing and defaults
    select
        *,
        upper(method) as method_upper,
        coalesce(method, 'N/A') as resolved_method
    from payments
    where order_id is not null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        first_value(amount, true) over w_part as first_known_amount,
        last_value(method) ignore nulls over (partition by method order by amount, id rows between unbounded preceding and current row) as last_known_method,
        round(amount / sum(amount) over (partition by status), 4) as amount_share
    from base
    window w_part as (order by amount, id rows between unbounded preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select *, cast(first_known_amount as int) as first_known_amount_int
    from metrics
    where method is not null
)
-- Grouped metrics
select
    date_trunc('quarter', paid_at) as quarter_start,
    status,
    count_if(method = 'promptpay') as promptpay_events,
    sum(case when method = 'card' then amount_share else 0 end) as card_amount_share,
    sum(case when status = 'FAILED' then first_known_amount_int else 0 end) as failed_first_known_amount_int
from filtered
group by date_trunc('quarter', paid_at), status
order by failed_first_known_amount_int desc
