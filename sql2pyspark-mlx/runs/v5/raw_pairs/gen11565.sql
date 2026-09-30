select case when year(shipped_date) < 2024 then 'before_2024' when year(shipped_date) = 2024 then '2024' else 'after_2024' end as shipped_cohort, carrier, count(case when status = 'IN_TRANSIT' then 1 end) as in_transit_count
from shipments
group by shipped_cohort, carrier
having in_transit_count >= 3;
