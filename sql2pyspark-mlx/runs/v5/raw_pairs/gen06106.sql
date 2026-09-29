select * from (select status, method, amount from payments) src pivot (max(amount) as amount, count(1) as item_count for method in ('promptpay' as promptpay, 'paypal' as paypal))
