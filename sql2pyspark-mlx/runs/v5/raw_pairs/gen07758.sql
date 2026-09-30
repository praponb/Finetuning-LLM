select id, customer_id, greatest(shipping_fee, discount) as greatest_shipping_fee_discount
from orders
where amount is not null;
