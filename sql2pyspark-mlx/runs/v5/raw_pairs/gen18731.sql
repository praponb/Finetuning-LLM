select *, row_number() over (partition by supplier_id order by cost desc, id) as dedup_rank from products qualify dedup_rank = 1
