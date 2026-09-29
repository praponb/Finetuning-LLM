select id, explode_outer(coupon_codes) as element
from carts
