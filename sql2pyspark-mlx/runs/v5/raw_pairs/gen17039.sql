select id, balance, customer_id, status, round(balance * 100, 2) adjusted_balance from accounts where account_type not in ('SAVINGS', 'CHECKING');
