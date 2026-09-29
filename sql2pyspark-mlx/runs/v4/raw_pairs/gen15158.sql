select id, method, length(status) as status_length
from payments
where method = 'card'
