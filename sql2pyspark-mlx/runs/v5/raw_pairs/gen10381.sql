select is_flagged, sum(case when merchant = 'Amazon' then 1 else 0 end) as amazon_count, round(stddev(amount), 2) as stddev_amount
from transactions
where merchant = 'Grab' and (merchant = 'Amazon' or txn_type in ('FEE', 'DEBIT'))
group by is_flagged
order by amazon_count desc;
