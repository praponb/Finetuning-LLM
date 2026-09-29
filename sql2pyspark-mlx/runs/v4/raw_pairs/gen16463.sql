select region, city, opened_date from stores where not (region in ('South', 'North') or store_name like 'A%') order by city asc, region asc;
