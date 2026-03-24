# test/test_helpers.jl — Shared test setup helpers
#
# Include this file from runtests.jl before any test files that use these helpers.

using DuckDB, DBInterface

"""
    _setup_test_store(; legs=true, stations=true)::DuckDBStore

Create a DuckDBStore and insert standard test data (ORD→LHR leg + ORD/LHR stations).
Returns the store — caller is responsible for `close(store)`.
"""
function _setup_test_store(; legs::Bool=true, stations::Bool=true)
    store = DuckDBStore()

    if legs
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR', 540, 1320, 535, 1325,
            -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
            '2026-06-15', '2026-06-15', 127,
            'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
        )
        """)
    end

    if stations
        DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)")
        DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)")
    end

    post_ingest_sql!(store)
    return store
end
