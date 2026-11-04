{{
config(
    materialized='incremental',
    unique_key='date',
    incremental_strategy='merge',
    tblproperties={'delta.enableDeletionVectors': 'false'}
)


}}

with whales as (

SELECT 
    date,
    count_if(output_value> 100 ) as whale_txs,
    count_if(output_value> 10 ) as shark_txs,
    count(*) as total_txs,
    sum(if(output_value> 10, output_value, 0)) as big_btc,
    sum(case when output_value> 100 and input_count> 1 and output_count<= 2 then 1 else 0 end) as consolidation,
    sum(case when output_value> 100 and output_count> 2 then 1 else
    0 end) as dispersal,
    max(output_value) as top_tx_btc,
    sum(if(output_value> 100, virtual_size, 0)) as whale_vbytes,
    sum(virtual_size) as total_vbytes
FROM {{ source('btc_data', 'transactions') }}

WHERE 
    is_coinbase = false
    and date < current_date
    {% if is_incremental() %}
        AND date >= (SELECT date_add(MAX(date), -2 ) FROM {{ this }})
    {% else %}
        AND date >= current_date - 180
    {% endif %}
    

GROUP BY date
)

select 
    *,
    whale_txs - consolidation - dispersal as transfer,
    shark_txs - whale_txs as shark_only,
    whale_vbytes / nullif(total_vbytes, 0) as whale_vbyte_pct
from whales



