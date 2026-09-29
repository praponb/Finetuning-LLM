select *, round(amount * 1.07, 2) as adjusted_amount, case when is_flagged then 'Yes' else 'No' end as is_flagged_label from transactions;
