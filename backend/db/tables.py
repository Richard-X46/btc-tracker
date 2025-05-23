from sqlalchemy import Table, Column, Integer, String, DateTime, Float, BigInteger
from .meta import metadata  

blocks_table = Table(
    "blocks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("hash", String, unique=True, nullable=False),
    Column("time", DateTime, nullable=False),
    Column("median_time", DateTime),
    Column("size", Integer),
    Column("stripped_size", Integer),
    Column("weight", Integer),
    Column("version", Integer),
    Column("version_hex", String),
    Column("version_bits", String),
    Column("merkle_root", String),
    Column("nonce", BigInteger),
    Column("bits", String),
    Column("difficulty", Float),
    Column("chainwork", String),
    Column("coinbase_data_hex", String),
    Column("transaction_count", Integer),
    Column("witness_count", Integer),
    Column("input_count", Integer),
    Column("output_count", Integer),
    Column("input_total", BigInteger),
    Column("input_total_usd", Float),
    Column("output_total", BigInteger),
    Column("output_total_usd", Float),
    Column("fee_total", BigInteger),
    Column("fee_total_usd", Float),
    Column("fee_per_kb", Float),
    Column("fee_per_kb_usd", Float),
    Column("fee_per_kwu", Float),
    Column("fee_per_kwu_usd", Float),
    Column("cdd_total", Float),
    Column("generation", BigInteger),
    Column("generation_usd", Float),
    Column("reward", BigInteger),
    Column("reward_usd", Float),
    Column("guessed_miner", String),
)


if __name__ == "__main__":
    pass