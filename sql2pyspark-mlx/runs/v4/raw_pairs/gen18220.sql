select country from suppliers union all select country from sessions where device not in ('tablet', 'desktop', 'mobile');
