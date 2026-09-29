select
    case when amount >= 4500 then 'high' when amount >= 1300 then 'mid' else 'low' end as amount_bucket,
    txn_type,
    sum(amount) as total_amount,
    count(distinct amount) as unique_txn_types,
    round((100 * sum(case when merchant = '7-Eleven' then 1 else 0 end)) / count(*), 1) as pct_7_eleven
from transactions
where merchant in ('7-Eleven', 'Starbucks', 'Lazada')
group by case when amount >= 4500 then 'high' when amount >= 1300 then 'mid' else 'low' end, txn_type
order by txn_type, amount_bucket
limit 1
