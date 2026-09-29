select
    amount_bucket,
    account_id,
    avg_amount,
    max_amount
from (
    select
        case when amount >= 3900 then 'high' when amount >= 1200 then 'mid' else 'low' end as amount_bucket,
        account_id,
        round(avg(amount), 2) as avg_amount,
        max(amount) as max_amount
    from transactions
    group by case when amount >= 3900 then 'high' when amount >= 1200 then 'mid' else 'low' end, account_id
) as t
where avg_amount < 44
order by account_id desc;
