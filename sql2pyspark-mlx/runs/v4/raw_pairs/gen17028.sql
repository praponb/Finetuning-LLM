with metrics as (
    -- Ranking and gap features
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        ntile(5) over (partition by country order by age asc, id asc) as age_bucket,
        first_value(credit_limit) ignore nulls over (partition by segment order by age asc, id asc rows between unbounded preceding and current row) as first_known_credit_limit
    from customers
)
select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    case when segment rlike '^(SMB|Consumer)$' then 'GROUP_B' when segment rlike '^(Corporate)$' then 'CORE' else 'PRIORITY' end as segment_zone
from metrics;
