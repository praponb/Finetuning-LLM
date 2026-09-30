select *, row_number() over (partition by location order by budget, id) as dedup_rank from departments qualify dedup_rank = 1;
