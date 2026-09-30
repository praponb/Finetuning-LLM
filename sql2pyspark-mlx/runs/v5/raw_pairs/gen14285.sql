select
    id,
    name,
    city,
    signup_date,
    is_active,
    age,
    row_number() over (partition by segment order by age desc, id) as event_seq,
    round(age / sum(age) over (partition by segment), 4) as age_share
from customers;
