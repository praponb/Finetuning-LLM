select
    *,
    row_number() over (partition by status order by paid_at asc, id asc) row_seq,
    rank() over (partition by method order by amount asc nulls last) amount_rank
from payments;
