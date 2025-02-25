drop table if exists payme_sandbox.p2p_user_activation_sessions_24;

create table payme_sandbox.p2p_user_activation_sessions_24 as
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
    and j.date < u.reg_date + interval '24 hours'
    where j.date >= '2023-05-01' and j.date < '2025-04-01'
        and j.method = 'sessions.create'
)


select
    s.user_id,
    count(*) sessions_count
from user_sessions s
group by s.user_id