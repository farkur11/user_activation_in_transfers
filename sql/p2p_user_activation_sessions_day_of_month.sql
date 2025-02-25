
drop table if exists payme_sandbox.p2p_user_activation_sessions_day_of_month;
create table payme_sandbox.p2p_user_activation_sessions_day_of_month as

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

select user_id, day_of_month
    from
    (
    select user_id, day_of_month, rank() over (partition by user_id order by session_count desc) rn
        from
        (
        select u._id user_id, extract(day from j.date) as day_of_month, count(*) session_count
        from ods__mdbmn__payme.journal j
        join users u
            on j.user_id = u._id
        where j.date >= '2023-05-01' and j.date < '2025-04-01'
            and j.method = 'sessions.create'
        group by 1, 2
        having count(*) > 2
        ) s1
    ) s2
where rn = 1