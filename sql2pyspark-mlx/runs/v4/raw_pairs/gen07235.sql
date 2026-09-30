with top_accounts as (
    select
        id,
        customer_id,
        opened_date,
        row_number() over (partition by customer_id order by opened_date desc, id) rn
    from accounts
    where account_type = 'CREDIT'
)
select *
from top_accounts
where rn <= 3;
