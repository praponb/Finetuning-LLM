select
    merchant_group,
    is_flagged,
    7_eleven_count,
    grab_count
from (
    select
        case when merchant in ('Grab', 'Lazada') then 'group_a' else 'other' end as merchant_group,
        is_flagged,
        sum(case when merchant = '7-Eleven' then 1 else 0 end) as 7_eleven_count,
        count(case when merchant = 'Grab' then 1 end) as grab_count
    from transactions
    where hour(txn_time) between 6 and 13
    group by case when merchant in ('Grab', 'Lazada') then 'group_a' else 'other' end, is_flagged
) as agg
where 7_eleven_count > 59;
