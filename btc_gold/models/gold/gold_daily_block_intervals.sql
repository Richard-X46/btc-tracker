{{ config(materialized='incremental', unique_key='date', incremental_strategy='merge', tblproperties={'delta.enableDeletionVectors': 'false'}) }}

-- Full history, no lower bound (wide range per request). Grain: one row/day.
-- interval_sec from mediantime lag by block number; bounds drop reorg
-- artefacts (<=0) and outliers (>=7200s). Expect ~10min baseline; Jun-Jul 2021
-- China-ban shock should read well above it.
-- Incremental re-merges the trailing 7 days (reorg buffer); the first block
-- inside the window has no predecessor and drops out via the >0 filter.

with b as (
    select
        date,
        number,
        unix_timestamp(mediantime) - unix_timestamp(lag(mediantime) over (order by number)) as interval_sec
    from {{ source('btc_data', 'blocks') }}
    where
        to_date(date) < current_date
        {% if is_incremental() %}
        and to_date(date) >= (select date_add(max(to_date(date)), -7) from {{ this }})
        {% endif %}
)

select
    date,
    avg(interval_sec) / 60 as avg_min,
    count(*) as blocks
from b
where interval_sec > 0 and interval_sec < 7200
group by date
