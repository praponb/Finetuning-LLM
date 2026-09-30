select id, amount, merchant, account_id, case when is_flagged then 'Yes' else 'No' end as is_flagged_label from transactions where merchant in ('7-Eleven', 'Starbucks') order by merchant
