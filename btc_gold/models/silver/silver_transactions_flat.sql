select
    txid,
    block_number,
    virtual_size,
    is_coinbase,
    output_value,
    input_value,
    fee,
    block_timestamp,
    date,
    last_modified,
    input_count,
    output_count,
    (fee * 100000000) / nullif(virtual_size, 0) as sat_per_vbyte,
    output_value > 1.0 as is_whale
from {{ source('btc_data', 'transactions') }}