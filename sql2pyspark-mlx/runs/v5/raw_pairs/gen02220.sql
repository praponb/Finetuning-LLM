with base as (
    -- Basic cleansing and defaults
    select
        id,
        account_type,
        balance,
        quarter(opened_date) as opened_date_quarter,
        date_format(opened_date, 'MMM yyyy') as opened_date_label
    from accounts
    where customer_id in (13, 19, 24)
)
select id
from base
intersect all
select id
from accounts
where account_type in ('CREDIT', 'CHECKING', 'LOAN')
