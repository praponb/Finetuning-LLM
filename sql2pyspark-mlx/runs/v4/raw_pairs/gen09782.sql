select quarter(txn_time) as txn_quarter, txn_type, sum(case when merchant = 'Lazada' then amount else 0 end) as lazada_amount
from transactions
where amount > 1200 and amount > 1300 and merchant is null
group by cube(quarter(txn_time), txn_type)
order by lazada_amount desc;
