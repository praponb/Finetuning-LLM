with scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (order by starts_on desc, id) as event_seq,
        avg(discount_pct) over w_grp as discount_pct_avg_6_prior
    from promotions
    window w_grp as (order by ends_on, id rows between 6 preceding and 1 preceding)
)
select
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    event_seq,
    discount_pct_avg_6_prior
from scored;
