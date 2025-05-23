"""
core engine file 


- mentioning the list of columns in the tables extracted from the blocks api from blockchair 
which serves as the source of truth for the data schema

"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from backend.db.meta import metadata
from backend.db.tables import blocks_table
load_dotenv()


# column reference for the blocks table
columns = [
    "id",  # Unique identifier for the block in the Blockchair database
    "hash",  # SHA-256 hash of the block header; serves as the block's unique fingerprint
    "time",  # Timestamp indicating when the block was mined (Unix time)
    "median_time",  # Median of the timestamps of the previous 11 blocks; used for locktime calculations
    "size",  # Total size of the block in bytes
    "stripped_size",  # Size of the block excluding witness data (used in SegWit)
    "weight",  # Block weight as defined in BIP 141; calculated as (stripped_size * 3) + total_size
    "version",  # Block version number indicating the set of block validation rules to follow
    "version_hex",  # Hexadecimal representation of the block version
    "version_bits",  # Specific bits set in the version field to signal soft forks (per BIP 9)
    "merkle_root",  # Root of the Merkle tree of all transactions in the block
    "nonce",  # 32-bit number used by miners to vary the block header hash
    "bits",  # Target threshold in compact format; represents the difficulty
    "difficulty",  # Current difficulty target for mining the block
    "chainwork",  # Total cumulative work in the blockchain up to and including this block
    "coinbase_data_hex",  # Hex-encoded data from the coinbase transaction's scriptSig; often contains miner-specific data
    "transaction_count",  # Total number of transactions in the block
    "witness_count",  # Number of transactions that include witness data (SegWit)
    "input_count",  # Total number of inputs across all transactions in the block
    "output_count",  # Total number of outputs across all transactions in the block
    "input_total",  # Sum of all input values in satoshis
    "input_total_usd",  # Estimated USD value of all inputs at the time of the block
    "output_total",  # Sum of all output values in satoshis
    "output_total_usd",  # Estimated USD value of all outputs at the time of the block
    "fee_total",  # Total transaction fees in satoshis (input_total - output_total)
    "fee_total_usd",  # Estimated USD value of total transaction fees
    "fee_per_kb",  # Average fee per kilobyte in satoshis
    "fee_per_kb_usd",  # Average fee per kilobyte in USD
    "fee_per_kwu",  # Average fee per kilo weight unit in satoshis
    "fee_per_kwu_usd",  # Average fee per kilo weight unit in USD
    "cdd_total",  # Total Coin Days Destroyed; a measure of transaction activity
    "generation",  # Amount of newly generated coins in satoshis (block subsidy)
    "generation_usd",  # Estimated USD value of newly generated coins
    "reward",  # Total reward for the block (generation + fees) in satoshis
    "reward_usd",  # Estimated USD value of the total reward
    "guessed_miner",  # Name of the mining pool or miner, inferred from coinbase data
]

# --- Configuration ---

DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
engine = create_engine(DB_URL, echo=False)

# --- Metadata and Inspector ---

metadata.reflect(bind=engine)

inspector = inspect(engine)


# list all tables with inspector
def list_tables():
    """
    List all tables in the database.
    """
    return inspector.get_table_names()


# list all columns in passed table
def list_columns(table_name):
    """
    List all columns in a given table.
    """
    return inspector.get_columns(table_name)



# if inspector.has_table("blocks"):
#     blocks_table.drop(bind=engine)
#     print("Table 'blocks' dropped successfully.")
# else:
#     print("Table 'blocks' does not exist.")

# Create the blocks table
metadata.create_all(engine)