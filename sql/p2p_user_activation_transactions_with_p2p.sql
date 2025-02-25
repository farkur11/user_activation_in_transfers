
drop table if exists payme_sandbox.p2p_user_activation_transactions_with_p2p;
create table payme_sandbox.p2p_user_activation_transactions_with_p2p as
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


select t1.*,
date_part('day', t1.create_time - users.reg_date) as days_after_registration,
row_number() over (partition by payer_id order by create_time) rn
from
    (
    select *
    from
        (
        select r.payer_id, create_time,
        case
            when m.cashboxtype = 'credit' then 'credit'
            when r."type" = '5' and meta_owner is null then 'p2p'
            when r.merchant in ('5bdc2a21489ce62470205ed9', '649ae49708a6047d3cd9eb73', '6641fe86fe41a3907df96e7a') then 'monitoring'
            when r.merchant in ('5d5a7eb4ef85dc207414fa3b', '64c7afcdd9109371f8bf03fb', '664200d1fe41a3907df96eac') then 'gubdd'
            when r.merchant in ('66e3f2e943059d2bd918e261', '66e3f2ff43059d2bd918e265') then 'payme_plus'
            else i.title_ru
            end tr_type,
            amount/100 amount
        from ods__mdbmn__paycom.receipts r
        join users u
        on r.payer_id = u._id
            and r.create_time - u.reg_date < interval '9 months'
        left join ods__mdbmn__paycom.merchants m
        on r.merchant = m._id
        left join ods__mdbmn__paycom.business b
    on m.business_id = b._id
    left join
        (
        select
            i."_id",
        case
            when i2.title_ru = 'Услуги связи' then 'mobile'
            when i2.title_ru = 'Коммунальные услуги' then 'utilities'
            when i2.title_ru = 'Интернет и ТВ-провайдеры' then 'internet_tv'
            when i2.title_ru = 'Государственные услуги' then 'gov_services'
            when i2.title_ru = 'Кафе и рестораны' then 'cafe_restaurants'
            when i2.title_ru = 'Финансовые услуги' then 'financial_services'
            when i2.title_ru = 'FMCG' then 'consumer_goods_or_groceries'
            when i2.title_ru = 'Транспорт' then 'transport'
            when i2.title_ru = 'Косметика и парфюмерия' then 'health_beauty_products'
            when i2.title_ru = 'Образование' then 'education'
            when i2.title_ru = 'Турагенты и туроператоры' then 'tourism'
            when i2.title_ru = 'Бытовая техника и электроника' then 'electronic_equipments'
            when i2.title_ru = 'Красота и здоровье' then 'health_beauty_products'
            when i2.title_ru = 'Одежда и обувь' then 'fashion'
            when i2.title_ru = 'Развлечение и отдых' then 'entertainment'
            when i2.title_ru = 'АЗС' then 'gas_stations'
            when i2.title_ru = 'Аптеки' then 'pharmacy'
            when i2.title_ru = 'Благотворительность' then 'charity'
            else 'other payments'
            end as title_ru
        from
            ods__mdbmn__paycom.industries i
        left join
            (
                select _id, title_ru
                from ods__mdbmn__paycom.industries
                where parent is null
             ) i2
        on
            coalesce(i.parent, i._id) = i2._id
        ) i
    on b.industry = i._id
    where create_time >= '2023-01-01' and create_time < '2025-01-01'
        and r."state" = '4'
        and r."type" in ('1', '2', '3', '4', '5', '7')
        and r.payment_service = '56e7ce796b6ef347d846e3eb'
        and external = false
    ) rec

union all

    select user_id payer_id, min(create_time) create_time, 'my_home' tr_type, count(*) amount
    from ods__mdbmn__payme.myhome h
    join users u
    on h.user_id = u._id
    and h.create_time - u.reg_date < interval '9 months'
    where create_time >= '2023-01-01' and create_time < '2025-01-01'
    group by 1,3

    union all

    select account payer_id, min(createdat) create_time, 'identification' tr_type, count(*) amount
    from ods__mdbrt__identification.operator_tickets i
    join users u
    on i.account = u._id
    and i.createdat - u.reg_date < interval '9 months'
    where identificationstate = '5'
    group by 1, 3
) t1
left join users
on t1.payer_id = users._id
;