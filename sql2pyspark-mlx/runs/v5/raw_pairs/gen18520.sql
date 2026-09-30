select *
from (
    select year(order_date) as order_year, round(stddev(shipping_fee), 2) as stddev_shipping_fee, max(amount) as max_amount
    from orders
    group by year(order_date)
) as x
where max_amount >= 950
order by stddev_shipping_fee desc
