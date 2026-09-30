select * from (select txn_type, merchant, amount from transactions) pivot (max(amount) for merchant in ('7-Eleven', 'Shell'));
