drop table if exists payme_sandbox.p2p_user_activation_classification;


create table payme_sandbox.p2p_user_activation_classification as
with rec as (
select payer_id, create_time, amount
from ods__mdbmn__paycom.receipts r
    where r.create_time >= '2023-01-01'
    and r.create_time < '2025-04-01'
    and r."state" = '4'
    and r."type" = '5'
    and r.payment_service = '56e7ce796b6ef347d846e3eb'
    and r.external = false
    and r.meta_owner is null
    and r.dwh_deleted_flg = false
    )
,
users as (
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

, rec2 as (
    select rec.payer_id,
           users.reg_date,
           rec.create_time,
           rec.amount/100 amount,
           extract(epoch from create_time - reg_date)/60/60/24 days_after_reg
        from rec
        join users
    on rec.payer_id = users._id
        )

select payer_id, reg_date, create_time, days_after_reg, amount
    from rec2
where create_time::date >= reg_date::date
    and create_time - reg_date < interval '9 months'
    and reg_date < '2024-04-01'