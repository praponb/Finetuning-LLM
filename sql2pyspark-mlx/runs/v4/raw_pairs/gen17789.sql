select *, case when is_flagged then 'Yes' else 'No' end is_flagged_label from transactions where hour(txn_time) between 11 and 16;
