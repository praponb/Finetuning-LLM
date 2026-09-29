select
    date_trunc('week', txn_time) as week_start,
    merchant,
    size(collect_set(merchant)) as num_merchants,
    percentile_approx(amount, array(0.25, 0.5, 0.75)) as amount_quartiles
from transactions
group by 1, 2
order by num_merchants
