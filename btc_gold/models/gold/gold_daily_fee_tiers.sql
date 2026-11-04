{{ config(materialized='incremental', unique_key='date', incremental_strategy='merge', tblproperties={'delta.enableDeletionVectors': 'false'}) }}

-- Trailing-365d rolling window; shares not levels (shows the market maturing).
-- Grain: one row/day. Tiers on fee*1e8/virtual_size: <1 / 1-5 / 5-20 / 20+ sat/vb.
-- Incremental re-merges trailing 2 days (reorg buffer). Rows older than 365d go
-- stale (merge never deletes) — dashboard filters date >= current_date - 365.

with f as (
    select
        date,
        fee * 1e8 / virtual_size as sat_vb
    from {{ source('btc_data', 'transactions') }}
    where
        is_coinbase = false
        and virtual_size > 0
        and fee is not null
        and to_date(date) < current_date
        {% if is_incremental() %}
        and to_date(date) >= (select date_add(max(to_date(date)), -2) from {{ this }})
        {% else %}
        and to_date(date) >= current_date - 365
        {% endif %}
)

select
    date,
    avg(case when sat_vb < 1 then 1.0 else 0.0 end) as t_sub1,
    avg(case when sat_vb >= 1 and sat_vb < 5 then 1.0 else 0.0 end) as t_1_5,
    avg(case when sat_vb >= 5 and sat_vb < 20 then 1.0 else 0.0 end) as t_5_20,
    avg(case when sat_vb >= 20 then 1.0 else 0.0 end) as t_20p,
    count(*) as txs
from f
group by date
