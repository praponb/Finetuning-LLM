with latest as (
    select *
    from (
        select *, row_number() over (partition by city, segment order by signup_date desc, id) as dedup_rank
        from customers
    ) t
    where dedup_rank = 1
)
select
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    case when segment rlike '^(SMB|Corporate)$' then 'GROUP_B' when segment rlike '^(Consumer)$' then 'CORE' else 'PRIORITY' end as segment_zone
from latest;
