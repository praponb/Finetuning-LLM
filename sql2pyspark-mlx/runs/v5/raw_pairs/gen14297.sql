select s.*
from orders s
where exists (
        select 1
        from payments x
        where x.order_id = s.id
          and x.paid_at <= s.order_date
    );
