with recent_stores as (
    select *
    from stores
    where city = 'Tokyo' and (region not in ('North', 'Central', 'South') or sqft >= 13038)
),
opened_cohort_summary as (
    select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, max(sqft) as max_sqft
    from recent_stores
    group by case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end
)
select opened_cohort, max_sqft
from opened_cohort_summary
order by opened_cohort
limit 50
