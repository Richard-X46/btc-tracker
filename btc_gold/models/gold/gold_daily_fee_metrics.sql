{{ config(materialized='table') }}

with fee_stats as (
    select
        date,
        percentile_approx(sat_per_vbyte, 0.5) as p50_sat_vbyte,
        percentile_approx(sat_per_vbyte, 0.9) as p90_sat_vbyte,
        percentile_approx(sat_per_vbyte, 0.99) as p99_sat_vbyte,
        count(*) as tx_count
    from {{ ref('silver_transactions_flat') }}
    where is_coinbase = false
    group by date
),

block_stats as (
    select
        date,
        avg(weight / 4000000.0 * 100) as avg_block_fullness_pct,
        count(*) as block_count
    from {{ source('btc_data', 'blocks') }}
    group by date
)

select
    f.date,
    f.p50_sat_vbyte,
    f.p90_sat_vbyte,
    f.p99_sat_vbyte,
    f.tx_count,
    b.avg_block_fullness_pct,
    b.block_count
from fee_stats f
left join block_stats b on f.date = b.date