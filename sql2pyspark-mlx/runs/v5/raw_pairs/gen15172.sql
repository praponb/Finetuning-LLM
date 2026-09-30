select * from (select status, method, amount from payments)
pivot (max(amount) for method in ('card', 'cash', 'promptpay', 'bank_transfer'));
