with valid as (
    -- Keep recent, valid rows
    select
        *,
        length(status) status_length,
        upper(status) status_upper
    from payments
    where paid_at >= date '2023-11-02'
      and order_id is null
)
-- Window metrics over several different partitions
select
    *,
    avg(status_length) over (partition by order_id, method order by paid_at asc, id asc rows between 2 preceding and current row) status_length_avg_2,
    lead(status_length, 2) over (partition by status order by paid_at asc, id asc) next_status_length
from valid
