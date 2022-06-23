use rusqlite::{Connection, Transaction};

pub fn migrate(conn: &mut Connection) -> rusqlite::Result<()> {
    let mut tx = conn.transaction()?;

    let user_version: usize =
        tx.query_row("SELECT * FROM pragma_user_version", [], |r| r.get(0))?;

    let total = MIGRATIONS.len();
    for (i, migration) in MIGRATIONS.iter().enumerate().skip(user_version) {
        println!("Migrating vault from {} to {} (out of {})", i, i + 1, total);
        migration(&mut tx)?;
    }

    tx.pragma_update(None, "user_version", total)?;
    tx.commit()
}

const MIGRATIONS: [fn(&mut Transaction) -> rusqlite::Result<()>; 1] = [m1];

fn m1(tx: &mut Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        "
        CREATE TABLE euph_msgs (
            -- Message
            room              TEXT NOT NULL,
            id                INT  NOT NULL,
            parent            INT,
            previous_edit_id  INT,
            time              INT  NOT NULL,
            content           TEXT NOT NULL,
            encryption_key_id TEXT,
            edited            INT,
            deleted           INT,
            truncated         INT  NOT NULL,

            -- SessionView
            user_id             TEXT NOT NULL,
            name                TEXT,
            server_id           TEXT NOT NULL,
            server_era          TEXT NOT NULL,
            session_id          TEXT NOT NULL,
            is_staff            INT  NOT NULL,
            is_manager          INT  NOT NULL,
            client_address      TEXT,
            real_client_address TEXT,

            PRIMARY KEY (room, id)
        ) STRICT;

        CREATE TABLE euph_spans (
            room  TEXT NOT NULL,
            start INT,
            end   INT,

            PRIMARY KEY (room, start, end),
            FOREIGN KEY (room, start) REFERENCES euph_msgs (room, start),
            FOREIGN KEY (room, end) REFERENCES euph_msgs (room, end)
        ) STRICT;

        CREATE VIEW euph_trees (room, id) AS
        SELECT room, id
        FROM euph_msgs
        WHERE parent IS NULL
        UNION
        SELECT *
        FROM (
            SELECT room, parent
            FROM euph_msgs
            WHERE parent IS NOT NULL
            EXCEPT
            SELECT room, id
            FROM euph_msgs
        );
        ",
    )
}