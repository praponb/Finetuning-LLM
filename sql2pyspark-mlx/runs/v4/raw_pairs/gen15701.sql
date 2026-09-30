select
    id,
    credit_limit,
    dense_rank() over (order by credit_limit) as credit_limit_dense_rank,
    row_number() over (order by credit_limit, id) as row_num
from customers
order by credit_limit_dense_rank;
