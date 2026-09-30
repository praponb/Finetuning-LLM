with cleaned as (
    -- Basic cleansing and defaults
    select *, concat_ws('-', account_type, status) as account_type_status_key
    from accounts
    where opened_date > date '2023-05-06'
      and status is not null
)
-- Attach customers attributes
select
    src.id,
    src.customer_id,
    src.balance,
    src.opened_date,
    src.account_type_status_key,
    c.email,
    c.name,
    c.country
from cleaned src
inner join customers c
    on src.customer_id <=> c.id;
