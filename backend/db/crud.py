from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from backend.db.tables import blocks_table

def upsert_blocks(conn, data_list: list[dict]):
    """Bulk UPSERT: insert or update rows on PK conflict."""
    stmt = pg_insert(blocks_table).values(data_list)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={col: stmt.excluded[col] for col in blocks_table.c.keys()},
    )
    conn.execute(stmt)

def insert_blocks_skip_dups(conn, data_list: list[dict]):
    """Bulk INSERT, skipping rows on PK conflict."""
    stmt = pg_insert(blocks_table).values(data_list)
    stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
    conn.execute(stmt)

def get_block_by_id(conn, block_id: int):
    """Fetch a single block by its ID."""
    stmt = select(blocks_table).where(blocks_table.c.id == block_id)
    return conn.execute(stmt).first()

def delete_block(conn, block_id: int):
    """Delete a block by its ID."""
    stmt = delete(blocks_table).where(blocks_table.c.id == block_id)
    conn.execute(stmt)