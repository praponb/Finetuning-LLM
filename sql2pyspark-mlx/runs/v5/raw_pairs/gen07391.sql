with ranked as (
    select
        id,
        account_type,
        opened_date,
        status,
        row_number() over (partition by account_type order by opened_date desc, id) as row_num
    from accounts
)
select
    id,
    account_type,
    opened_date,
    status
from ranked
where row_num <= 3;
