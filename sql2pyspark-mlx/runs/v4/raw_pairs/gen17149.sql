select account_type, concat_ws(', ', sort_array(collect_set(account_type))) as account_type_values from accounts group by account_type order by account_type;
