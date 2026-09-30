select
    id,
    budget,
    lag(budget) over (partition by dept_name order by budget, id) as prev_budget,
    lead(budget) over (partition by dept_name order by budget, id) as next_budget,
    row_number() over (partition by dept_name order by budget, id) as row_seq,
    row_number() over (order by budget, id) as row_seq_2
from departments;
