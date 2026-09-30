select *
from transactions
where amount <= 2900 and is_flagged = false and merchant in ('Grab', 'Amazon');
