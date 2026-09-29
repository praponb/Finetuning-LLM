with valid as (
    -- Basic cleansing and defaults
    select
        *,
        trim(name) clean_name,
        greatest(age, credit_limit) greatest_age_credit_limit
    from customers
    where signup_date >= date '2023-03-18'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        clean_name,
        greatest_age_credit_limit
    from valid
    qualify row_number() over (partition by segment order by signup_date nulls first, id) = 1
),
latest as (
    -- QUALIFY-style window filter
    select *
    from deduped
    qualify row_number() over (partition by city order by credit_limit desc, id) = 1
),
metrics as (
    -- Ranking and gap features
    select
        id,
        city,
        credit_limit,
        age,
        lead(greatest_age_credit_limit) over (partition by city, segment order by age, id) next_greatest_age_credit_limit,
        lead(credit_limit) over (partition by city, segment order by age, id) next_credit_limit,
        lag(credit_limit) over (partition by city, segment order by age, id) prev_credit_limit,
        row_number() over (partition by city, segment order by age, id) event_seq,
        row_number() over (partition by segment order by greatest_age_credit_limit desc, id) seq_no
    from latest
)
-- Aggregate with conditional metrics
select
    seq_no,
    bit_or(age) as age_bit_or,
    count(*) filter (where city = 'Bangkok') as bangkok_rows,
    sort_array(collect_set(city)) as city_list,
    approx_percentile(next_greatest_age_credit_limit, 0.5) as next_greatest_age_credit_limit_p50
from metrics
group by seq_no
order by bangkok_rows;
