select status from orders where amount is not null union select status from payments order by status;
