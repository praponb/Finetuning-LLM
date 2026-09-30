select *, row_number() over (partition by country order by credit_limit desc nulls first, id) as dedup_rank
from customers
qualify dedup_rank = 1
