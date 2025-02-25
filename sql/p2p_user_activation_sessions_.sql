
drop table if exists payme_sandbox.p2p_user_activation_sessions_;

create table payme_sandbox.p2p_user_activation_sessions_ as
with users as (
    select _id, to_timestamp((replace(date, '.0', '')::numeric)/1000) reg_date
    from ods__mdbmn__payme.users u
    where to_timestamp((replace(date, '.0', '')::numeric)/1000) >= '2023-05-01'
        and to_timestamp((replace(date, '.0', '')::numeric)/1000) < '2025-01-01'
    union all
    select _id, to_timestamp((replace(date, '.0', '')::numeric)/1000) reg_date
    from ods__mdbmn__payme.users_archive u
        where to_timestamp((replace(date, '.0', '')::numeric)/1000) >= '2023-05-01'
        and to_timestamp((replace(date, '.0', '')::numeric)/1000) < '2025-01-01'
    )

, user_sessions as (
    select
        u._id as user_id,
        j.date,
        row_number() over (partition by u._id order by j.date) as session_rank,
        extract(hour from j.date) as session_hour
    from ods__mdbmn__payme.journal j
    join users u
        on j.user_id = u._id
    and j.date < u.reg_date + interval '90 days'
    where j.date >= '2023-05-01' and j.date < '2025-04-01'
        and j.method = 'sessions.create'
),

day_night_sessions as (
    select
        u._id as user_id,
        sum(case when extract(hour from j.date) between 6 and 18 then 1 else 0 end) as day_sessions,
        sum(case when extract(hour from j.date) not between 6 and 18 then 1 else 0 end) as night_sessions
    from ods__mdbmn__payme.journal j
    join users u
        on j.user_id = u._id
    where j.date >= '2023-05-01' and j.date < '2025-04-01'
        and j.method = 'sessions.create'
    group by u._id
)


select
    s.user_id,
    min(s.date) as session_date_1,
    min(case when session_rank = 2 then s.date end) as session_date_2,
    min(case when session_rank = 5 then s.date end) as session_date_5,
    min(case when session_rank = 10 then s.date end) as session_date_10,
    case
        when (max(case when session_rank = 2 then s.date end) - max(case when session_rank = 1 then s.date end)) = interval '1 day'
        then 1
        else 0
    end as returned_next_day,
    d.day_sessions,
    d.night_sessions,
    count(*) sessions_count
from user_sessions s
left join day_night_sessions d
    on s.user_id = d.user_id
group by s.user_id, d.day_sessions, d.night_sessions
;