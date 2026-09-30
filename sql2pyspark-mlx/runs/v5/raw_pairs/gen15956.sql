select
    id,
    plan,
    monthly_fee,
    row_number() over (partition by plan order by monthly_fee desc, id) as row_num
from subscriptions
