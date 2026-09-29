select s.*
from customers s
where not exists (
        select 1
        from subscriptions ch
        where ch.customer_id = s.id
          and ch.start_date >= s.signup_date
    )
