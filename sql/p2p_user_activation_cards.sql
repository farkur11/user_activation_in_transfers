
drop table if exists payme_sandbox.p2p_user_activation_cards;
create table payme_sandbox.p2p_user_activation_cards as

with users as (
    select _id, to_timestamp((replace(date, '.0', '')::numeric)/1000) reg_date
    from ods__mdbmn__payme.users u
    where to_timestamp((replace(date, '.0', '')::numeric)/1000) >= '2023-01-01'
        and to_timestamp((replace(date, '.0', '')::numeric)/1000) < '2025-01-01'
    union all
    select _id, to_timestamp((replace(date, '.0', '')::numeric)/1000) reg_date
    from ods__mdbmn__payme.users_archive u
        where to_timestamp((replace(date, '.0', '')::numeric)/1000) >= '2023-01-01'
        and to_timestamp((replace(date, '.0', '')::numeric)/1000) < '2025-01-01'
    )

, cards as
(select user_id, date, vendor_info_processing
    from
(
    select  user_id, date, vendor_info_processing, row_number() over (partition by user_id order by date) rn
    from
        (
        select user_id, date, vendor_info_processing
        from ods__mdbmn__payme.cards u
        where date >= '2023-01-01'
        and date < '2025-01-01'
        and active = true
        and vendor_info_processing in ('Uzcard', 'Humo')
        union all
        select user_id, date, vendor_info_processing
        from ods__mdbmn__payme.cards_archive u
        where date >= '2023-01-01'
        and date < '2025-01-01'
        and active = true
        and vendor_info_processing in ('Uzcard', 'Humo')
        ) c1
) c2
where rn = 1 )

select c.user_id, c.vendor_info_processing, date_part('day', c.date - u.reg_date) card_add_days, c.date, u.reg_date
from users u
join cards c
on u._id = c.user_id