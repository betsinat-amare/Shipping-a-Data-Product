-- Ensure that no messages have a date in the future relative to the current run time
select *
from {{ ref('stg_telegram_messages') }}
where message_date > current_timestamp
