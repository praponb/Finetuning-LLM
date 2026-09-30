with RankedAccounts as (
    select
        id,
        account_type,
        opened_date,
        row_number() over (partition by account_type order by opened_date desc, id) as row_num
    from accounts
)
select id, account_type, opened_date
from RankedAccounts
where row_num <= 3;
