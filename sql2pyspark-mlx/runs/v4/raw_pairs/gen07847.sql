with valid as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        level,
        cast(is_remote as int) as is_remote_flag,
        upper(level) as level_upper,
        coalesce(job_title, 'N/A') as job_title_clean
    from employees
    where level is not null
      and manager_id is not null
),
with_windows as (
    select
        *,
        row_number() over (partition by dept_id, job_title order by is_remote_flag, id) as seq_no,
        last_value(is_remote_flag, true) over (partition by level order by is_remote_flag, id rows between unbounded preceding and current row) as last_known_is_remote_flag,
        cume_dist() over w_recent as is_remote_flag_cume_dist
    from valid
    window w_recent as (partition by dept_id order by is_remote_flag nulls first)
),
remaining_keys as (
    select dept_id, job_title
    from with_windows
    intersect
    select dept_id, job_title
    from employees
    where dept_id = 24
),
verified as (
    select s.*
    from with_windows s
    inner join remaining_keys k on s.dept_id = k.dept_id and s.job_title = k.job_title
),
with_dims as (
    select s.*, p.dept_name
    from verified s
    left join departments p
        on s.dept_id = p.id
),
kept_keys as (
    select id
    from with_dims
    intersect
    select id
    from employees
    where dept_id = 12
),
verified_employees as (
    select s.*
    from with_dims s
    inner join kept_keys k on s.id = k.id
),
base as (
    select
        *,
        concat_ws('-', dept_name, level) as dept_name_level_key,
        upper(dept_name) as dept_name_upper
    from verified_employees
    where dept_id = 16
),
with_dims_employees as (
    select
        s.*,
        b.hire_date,
        b.full_name as employee_full_name,
        b.email
    from base s
    inner join employees b
        on s.manager_id = b.id
)
select
    id,
    full_name,
    dept_id,
    manager_id,
    job_title,
    level,
    is_remote_flag,
    level_upper,
    seq_no,
    case when job_title rlike '^(Manager|Intern)$' then 'GROUP_B' else 'EXTENDED' end as job_title_zone
from with_dims_employees
order by job_title_zone desc;
