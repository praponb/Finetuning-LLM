select
    product_id,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    stddev_samp(unit_price) as unit_price_stddev,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles_2,
    round(avg(unit_price), 2) as avg_unit_price,
    stddev_samp(quantity) as quantity_stddev
from order_items
group by 1
having unit_price_stddev < 1000000;
