with messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
),

channels as (
    select * from {{ ref('dim_channels') }}
),

final as (
    select
        m.message_id,
        c.channel_key,
        coalesce(d.date_key, cast(to_char(m.message_date, 'YYYYMMDD') as integer)) as date_key,
        m.message_text,
        length(m.message_text) as message_length,
        m.views as view_count,
        m.forwards as forward_count,
        m.has_media
    from messages m
    left join channels c on m.channel_name = c.channel_name
    left join dates d on date(m.message_date) = d.full_date
)

select * from final
