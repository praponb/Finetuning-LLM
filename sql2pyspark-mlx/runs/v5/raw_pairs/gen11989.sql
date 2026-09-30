with latest as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by location, dept_name order by budget desc) as rn
    from departments
    qualify rn = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        budget,
        rn,
        lag(budget) over w_ordered as prev_budget,
        row_number() over w_ordered as seq_no,
        row_number() over (partition by dept_name order by rn, id) as row_seq
    from latest
    window w_ordered as (partition by location order by rn, id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        budget,
        prev_budget,
        seq_no,
        row_seq,
        least(row_seq, seq_no) as least_row_seq_seq_no,
        cast(prev_budget as int) as prev_budget_int
    from with_windows
    where row_seq <= 8
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        budget,
        prev_budget,
        seq_no,
        row_seq + least_row_seq_seq_no as row_seq_plus_least_row_seq_seq_no,
        coalesce(prev_budget_int, 0) as prev_budget_int_filled
    from base
    where least_row_seq_seq_no < 52
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from filtered s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.salary <= 65000
        )
)
select id, seq_no
from not_flagged
