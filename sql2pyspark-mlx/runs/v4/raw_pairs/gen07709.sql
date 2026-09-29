select
    id,
    is_flagged,
    amount,
    cast(amount as int) as amount_int
from transactions
where amount <= 1100
