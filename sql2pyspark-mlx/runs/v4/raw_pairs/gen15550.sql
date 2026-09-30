select started_at
from sessions
where hour(started_at) between 8 and 13 and (browser = 'Safari' or device <> 'mobile');
