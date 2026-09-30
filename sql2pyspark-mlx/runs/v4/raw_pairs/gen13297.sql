select *, concat_ws('-', channel, status) as channel_status_key, least(shipping_fee, discount) as least_shipping_fee_discount
from orders
