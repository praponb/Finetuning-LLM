select *, row_number() over (partition by account_id order by amount asc, id asc) as dedup_rank from transactions qualify dedup_rank = 1;
