select credit_limit, regexp_extract(email, '@(.+)$', 1) as email_domain, initcap(trim(email)) as email_title
from customers;
