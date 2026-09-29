with valid as (
    -- Basic cleansing and defaults
    select *, upper(merchant) as merchant_upper
    from transactions
    where txn_time > date '2023-07-04'
)
-- Attach accounts attributes
select
    src.*,
    p.status,
    p.balance
from valid src
left join accounts p
    on src.account_id = p.id
where p.account_type in ('CHECKING', 'SAVINGS', 'CREDIT')
