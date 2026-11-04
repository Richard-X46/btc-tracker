{{ config(materialized='table', tblproperties={'delta.enableDeletionVectors': 'false'}) }}

-- Weekly refresh (Monday 09:00 UTC, after the daily job). NOT in the daily build.
-- Grain: week bucket x hour-of-day x day-of-week. Output ~2k rows, cheap to serve.
-- dow follows Spark convention: 1=Sunday .. 7=Saturday, both hr and dow bound to
-- block_timestamp so a future partition change can't split cells at midnight.

select
    date_trunc('week', to_date(date)) as wk,
    hour(block_timestamp) as hr,
    dayofweek(block_timestamp) as dow,
    percentile_approx(fee * 1e8 / virtual_size, 0.5) as med_sat_vb,
    percentile_approx(fee * 1e8 / virtual_size, 0.9) as p90_sat_vb,
    count(*) as txs
from {{ source('btc_data', 'transactions') }}
where
    is_coinbase = false
    and virtual_size > 0
    and fee is not null
    and to_date(date) >= current_date - 84
    and to_date(date) < current_date
group by 1, 2, 3
