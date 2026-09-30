with base as (
    select
        *,
        upper(account_type) as account_type_upper,
        abs(balance) as abs_balance
    from accounts
    where customer_id is not null
      and opened_date > to_date('2023-05-15')
)
select
    src.id,
    src.customer_id,
    src.account_type,
    src.balance,
    src.status,
    src.account_type_upper,
    c.name,
    c.email,
    c.signup_date
from base src
join customers c
    on src.customer_id <=> c.id;
