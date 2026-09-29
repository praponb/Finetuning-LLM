select id, method, amount, coalesce(amount, 0) as amount_filled, substring(status, 1, 2) as status_prefix from payments where method <> 'bank_transfer' or amount between 670 and 740;
