with filtered as (
    select
        id,
        account_type,
        opened_date,
        case when status in ('OPEN', 'FROZEN') then 'Priority' else 'Other' end as status_group
    from accounts
    where opened_date >= date '2023-07-09'
)
select *
from (
    select *, row_number() over (partition by account_type order by opened_date, id) as row_num
    from filtered
) x
where row_num = 1;
