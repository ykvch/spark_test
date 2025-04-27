import sqlite3
import pytest
import logging
import faker

FAKE = faker.Faker()
LOG = logging.getLogger(__name__)
ARTISTS_COUNT = 5


@pytest.fixture
def db_cursor():
    con = sqlite3.connect(":memory:")
    c = con.cursor()
    # NOTE: no foreign key to allow inconsistency
    c.execute("CREATE TABLE song(title text, year integer, artist_id integer)")
    c.execute("CREATE TABLE artist(name text, artist_id integer primary key)")
    yield c


@pytest.fixture
def sane_cursor(db_cursor: sqlite3.Cursor):
    """Songs matching artists in db"""
    artists = ", ".join(f"('{FAKE.name()}')" for _ in range(ARTISTS_COUNT))
    artists = [(FAKE.name(),) for _ in range(ARTISTS_COUNT)]
    LOG.debug(artists)
    db_cursor.executemany("INSERT INTO artist (name) VALUES (?)", artists)
    # Get artist ids to match when adding songs
    a_ids = db_cursor.execute("SELECT artist_id FROM artist").fetchall()

    songs = ", ".join(
        f"('{FAKE.catch_phrase()}', {2010 + i[0]}, {i[0]})" for i in a_ids
    )
    LOG.debug(songs)
    db_cursor.execute("INSERT INTO song VALUES " + songs)
    # con.commit()  no need for commit so far
    yield db_cursor


@pytest.fixture
def corr_cur(sane_cursor: sqlite3.Cursor):
    random_id = ", ".join(
        str(i[0])
        for i in sane_cursor.execute(
            "SELECT artist_id FROM artist ORDER BY RANDOM() LIMIT 2"
        ).fetchall()
    )
    sane_cursor.execute("DELETE FROM artist WHERE artist_id IN (?)", (random_id,))
    LOG.info("artist ids: %s", random_id)
    yield sane_cursor


def test_via_distinct(corr_cur: sqlite3.Cursor):
    # tables = corr_cur.execute("SELECT * FROM sqlite_master").fetchall()
    # LOG.warning("%s", tables)
    artists = corr_cur.execute("SELECT DISTINCT artist_id from artist").fetchall()
    LOG.warning("%s", artists)
    songs = corr_cur.execute("SELECT DISTINCT artist_id from song").fetchall()
    LOG.warning("%s", songs)
    # Test missing artist ids
    assert not set(songs) - set(artists)


def test_via_join(corr_cur: sqlite3.Cursor):
    missing_ids = corr_cur.execute(
        "SELECT song.artist_id from song LEFT JOIN artist "
        "ON artist.artist_id = song.artist_id "
        "WHERE artist.artist_id is NULL"
    ).fetchall()
    assert not missing_ids
