with valid as (
    select
        id,
        promo_code,
        starts_on,
        datediff(current_date(), starts_on) as days_since_starts_on
    from promotions
    where ends_on > date '2023-07-10'
),
ranked as (
    select id, starts_on
    from valid
    qualify row_number() over (partition by promo_code order by starts_on desc, id) = 1
),
enriched as (
    select
        id,
        starts_on,
        row_number() over (order by starts_on, id) as seq_no,
        row_number() over (order by starts_on desc, id) as event_seq
    from ranked
),
valid_promotions as (
    select *, date_add(starts_on, 67) as starts_on_plus_67d
    from enriched
    where event_seq > 6
),
cleaned as (
    select
        id,
        starts_on,
        seq_no,
        event_seq,
        starts_on_plus_67d,
        seq_no + event_seq as seq_no_plus_event_seq,
        round(event_seq * 1.15, 2) as adjusted_event_seq
    from valid_promotions
    where seq_no >= 5
),
enriched_promotions as (
    select
        *,
        ntile(3) over (order by adjusted_event_seq desc, id) as adjusted_event_seq_bucket,
        last_value(seq_no, true) over (order by seq_no, id rows between unbounded preceding and current row) as last_known_seq_no,
        avg(seq_no_plus_event_seq) over (order by starts_on, id rows between 3 preceding and current row) as seq_no_plus_event_seq_avg_3
    from cleaned
),
cleaned_promotions as (
    select
        *,
        datediff(starts_on, starts_on_plus_67d) as days_starts_on_plus_67d_to_starts_on,
        event_seq + seq_no_plus_event_seq as event_seq_plus_seq_no_plus_event_seq
    from enriched_promotions
    where adjusted_event_seq_bucket between 6 and 8
)
select
    id,
    metric,
    value
from (
    select
        id,
        event_seq_plus_seq_no_plus_event_seq,
        seq_no_plus_event_seq,
        adjusted_event_seq
    from cleaned_promotions
) src
unpivot (
    value for metric in (event_seq_plus_seq_no_plus_event_seq, seq_no_plus_event_seq, adjusted_event_seq)
);
