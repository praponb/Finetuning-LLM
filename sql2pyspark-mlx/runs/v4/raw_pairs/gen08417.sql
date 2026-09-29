select
    product_id,
    corr(unit_price, quantity) as corr_unit_price_quantity,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles
from order_items
group by product_id
order by corr_unit_price_quantity;
