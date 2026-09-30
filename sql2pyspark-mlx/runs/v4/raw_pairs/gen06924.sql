select status, sort_array(collect_set(status)) as status_list from accounts where balance is null and balance < 17000 and account_type in ('SAVINGS', 'LOAN') group by status order by status limit 5
