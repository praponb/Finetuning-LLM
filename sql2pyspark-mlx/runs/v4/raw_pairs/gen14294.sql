with kept_keys as (
    -- Set difference on keys
    select segment, city
    from customers
    except distinct
    select segment, city
    from customers
    where length(name) > 6
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from customers s
    join kept_keys k on s.segment = k.segment and s.city = k.city
)
select
    name,
    email,
    signup_date,
    is_active,
    credit_limit,
    case when name rlike '^(Mia Rossi|bob smith)$' then 'GROUP_B' else 'EXTENDED' end as name_zone
from reconciled
order by credit_limit nulls first;
