select
    status,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    size(collect_set(status)) as num_status,
    count(case when channel = 'mobile' then 1 end) as mobile_count,
    count(*) as num_rows,
    stddev_samp(shipping_fee) as shipping_fee_stddev,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles_2
from orders
group by status
