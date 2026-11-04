{{ config(materialized='incremental', unique_key='date', incremental_strategy='merge', tblproperties={'delta.enableDeletionVectors': 'false'}) }}

-- Subsidy 3.125 BTC/block (post-Apr-2024 halving; whole window qualifies).
-- fees_btc ex-coinbase; revenue = subsidy + fees; USD via daily close.


with fees as (
    select
        date,
        sum(fee) as fees_btc,
        sum(output_value) as settled_btc,
        count(*) as tx_count
    from {{ source('btc_data', 'transactions') }}
    where
        is_coinbase = false
        and fee is not null
        and to_date(date) < current_date
        {% if is_incremental() %}
        and to_date(date) >= (select date_add(max(to_date(date)), -2) from {{ this }})
        {% else %}
        and to_date(date) >= current_date - 180
        {% endif %}
    group by date
),

blks as (
    select
        date,
        count(*) as block_count
    from {{ source('btc_data', 'blocks') }}
    where
        to_date(date) < current_date
        {% if is_incremental() %}
        and to_date(date) >= (select date_add(max(to_date(date)), -2) from {{ this }})
        {% else %}
        and to_date(date) >= current_date - 180
        {% endif %}
    group by date
),

px as (
    select
        date,
        max_by(close, candle_start_ts) as btc_usd
    from {{ source('btc_data', 'price_minute') }}
    where
        to_date(date) < current_date
        {% if is_incremental() %}
        and to_date(date) >= (select date_add(max(to_date(date)), -2) from {{ this }})
        {% else %}
        and to_date(date) >= current_date - 180
        {% endif %}
    group by date
)

select
    f.date,
    f.fees_btc,
    f.settled_btc,
    f.tx_count,
    b.block_count,
    b.block_count * 3.125 as subsidy_btc,
    b.block_count * 3.125 + f.fees_btc as revenue_btc,
    (b.block_count * 3.125 + f.fees_btc) * p.btc_usd as revenue_usd,
    100 * f.fees_btc / (b.block_count * 3.125 + f.fees_btc) as fee_share_pct,
    p.btc_usd,
    f.fees_btc * p.btc_usd / f.tx_count as avg_fee_usd,
    10000 * f.fees_btc / nullif(f.settled_btc, 0) as friction_bps
from fees f
left join blks b on f.date = b.date
left join px p on f.date = p.date
