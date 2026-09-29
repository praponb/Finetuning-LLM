select merchant, sort_array(collect_set(merchant)) as merchant_list from transactions where (txn_time < '2023-04-01' and is_flagged) or not is_flagged group by merchant;
