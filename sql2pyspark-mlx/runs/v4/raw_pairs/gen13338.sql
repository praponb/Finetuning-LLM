with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at asc, id asc) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at asc, id asc) + interval 7 days then 1 else 0 end as is_new_burst
    from payments
)
-- Running sum of the flag = session number
select *, sum(is_new_burst) over (partition by status order by paid_at asc, id asc rows between unbounded preceding and current row) as session_id
from gaps;
