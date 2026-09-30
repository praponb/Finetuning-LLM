select *, trim(region) as clean_region, case when region = 'North' then 1 else 0 end as is_north from stores where region in ('South', 'East', 'West')
