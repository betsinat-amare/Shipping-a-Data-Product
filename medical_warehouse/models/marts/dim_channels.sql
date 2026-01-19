with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channels as (
    select distinct
        channel_name
    from stg_messages
),

final as (
    select
        {{ dbt.hash('channel_name') }} as channel_key,
        channel_name,
        -- Placeholder for channel type, could be enriched manually or via mapping
        case 
            when channel_name ilike '%cosmetics%' then 'Cosmetics'
            when channel_name ilike '%pharma%' then 'Pharmaceutical'
            else 'Medical' 
        end as channel_type
    from channels
)

select * from final
