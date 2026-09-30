select merchant, sort_array(collect_set(merchant)) as merchant_list
from transactions
where merchant = 'Lazada' and merchant = 'Amazon' and txn_time < '2024-03-14'
group by merchant
order by merchant desc
