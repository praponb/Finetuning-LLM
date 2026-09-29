select * from (select *, row_number() over (partition by account_id order by amount, id) as rank_in_group from transactions) ranked where rank_in_group = 1
