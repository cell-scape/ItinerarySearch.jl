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

# ── Fixture builders used across multiple test files ──────────────────────────
#
# These live here (not in test_ingest.jl where the SSIM/MCT ingest tests also
# use them) so every test file has access via `include("test_helpers.jl")`.
# That matters for the parallel runner — each worker runs one file and doesn't
# see Main-scope definitions from other files.

# Build a minimal valid SSIM file (Type 1 + Type 2 + Type 3 + Type 4 + Type 5)
function make_test_ssim()::String
    # Type 1: Header (200 bytes)
    t1 = rpad("1AIRLINE STANDARD SCHEDULE DATA SET", 194) * "000001"

    # Type 2: Carrier (200 bytes)
    t2 = rpad("2UUA        S2601JAN2631DEC2617MAR26", 194) * "000002"

    # Type 3: Flight leg (200 bytes)
    t3_fields = "3"                       # 1: record type
    t3_fields *= " "                      # 2: op suffix
    t3_fields *= "UA "                    # 3-5: airline
    t3_fields *= "1234"                   # 6-9: flight number
    t3_fields *= "01"                     # 10-11: itin var
    t3_fields *= "01"                     # 12-13: leg seq
    t3_fields *= "J"                      # 14: svc type
    t3_fields *= "01JAN26"                # 15-21: eff from
    t3_fields *= "31DEC26"                # 22-28: eff to
    t3_fields *= "1234567"                # 29-35: frequency
    t3_fields *= " "                      # 36: frequency rate
    t3_fields *= "ORD"                    # 37-39: dep station
    t3_fields *= "0900"                   # 40-43: pax dep
    t3_fields *= "0855"                   # 44-47: ac dep
    t3_fields *= "+0500"                  # 48-52: dep utc offset
    t3_fields *= "1 "                     # 53-54: dep terminal
    t3_fields *= "LHR"                    # 55-57: arr station
    t3_fields *= "2130"                   # 58-61: ac arr
    t3_fields *= "2145"                   # 62-65: pax arr
    t3_fields *= "+0000"                  # 66-70: arr utc offset
    t3_fields *= "2 "                     # 71-72: arr terminal
    t3_fields *= "789"                    # 73-75: eqp
    t3_fields *= rpad("JCDZPYBMEUHQVWST", 20)  # 76-95: prbd
    t3_fields *= rpad("", 24)             # 96-119: prbm, meal, jv airline
    t3_fields *= "DI"                     # 120-121: mct dep/arr
    t3_fields *= rpad("", 6)              # 122-127: spare
    t3_fields *= " "                      # 128: itin var overflow
    t3_fields *= "UA "                    # 129-131: aircraft owner
    t3_fields *= rpad("", 17)             # 132-148: spare
    t3_fields *= " "                      # 149: operating disclosure
    t3_fields *= rpad("", 11)             # 150-160: TRC
    t3_fields *= " "                      # 161: TRC overflow
    t3_fields *= rpad("", 10)             # 162-171: spare
    t3_fields *= " "                      # 172: spare
    t3_fields *= rpad("", 20)             # 173-192: ACV
    t3_fields *= "00"                     # 193-194: date var
    t3 = rpad(t3_fields, 194) * "000003"

    # Type 4: DEI record (DEI 50 — operating carrier)
    t4_fields = "4"
    t4_fields *= " "                      # 2: op suffix
    t4_fields *= "UA "                    # 3-5: airline
    t4_fields *= "1234"                   # 6-9: flight number
    t4_fields *= "01"                     # 10-11: itin var
    t4_fields *= "01"                     # 12-13: leg seq
    t4_fields *= "J"                      # 14: svc type
    t4_fields *= rpad("", 13)             # 15-27: spare
    t4_fields *= " "                      # 28: itin var overflow
    t4_fields *= "AA"                     # 29-30: board/off point indicators
    t4_fields *= "050"                    # 31-33: DEI code
    t4_fields *= "ORD"                    # 34-36: board point
    t4_fields *= "LHR"                    # 37-39: off point
    t4_fields *= rpad("BA 5678", 155)     # 40-194: data
    t4 = rpad(t4_fields, 194) * "000004"

    # Type 5: Trailer (200 bytes)
    t5_fields = "5"
    t5_fields *= " "
    t5_fields *= "UA "
    t5 = rpad(t5_fields, 187)
    t5 *= "000004"                        # 188-193: serial check
    t5 *= "E"                             # 194: end code
    t5 *= "000005"                        # 195-200: record serial

    join([t1, t2, t3, t4, t5], "\n") * "\n"
end

function make_test_mct()::String
    # Type 1: Header (200 bytes)
    t1 = rpad("1MINIMUM CONNECT TIME DATA SET", 194) * "000001"

    # Type 2: MCT record (200 bytes)
    # Station standard at ORD: 90 min II
    t2_fields = "2"                       # 1: record type
    t2_fields *= "ORD"                    # 2-4: arrival station
    t2_fields *= "0130"                   # 5-8: time HHMM (90 min)
    t2_fields *= "II"                     # 9-10: status
    t2_fields *= "ORD"                    # 11-13: departure station
    t2_fields *= rpad("", 81)             # 14-94: carrier/equipment/geographic/dates fields
    t2_fields *= "  "                     # 95-96: submitting carrier
    t2 = rpad(t2_fields, 194) * "000002"

    # Another MCT: exception at ORD for UA arrivals
    t3_fields = "2"
    t3_fields *= "ORD"                    # arr station
    t3_fields *= "0045"                   # 45 min
    t3_fields *= "DD"                     # status
    t3_fields *= "ORD"                    # dep station
    t3_fields *= "UA"                     # 14-15: arr carrier
    t3_fields *= rpad("", 79)             # 16-94
    t3_fields *= "UA"                     # 95-96: submitting carrier
    t3 = rpad(t3_fields, 194) * "000003"

    join([t1, t2, t3], "\n") * "\n"
end

# Build a fixed-width airport record (176 bytes) matching the MDSTUA schema.
function _make_airport_line(;
    country::String, state::String="00", airport::String,
    utc_var::String="+0000",
    lat_deg::String="00", lat_min::String="00", lat_sec::String="00", lat_hem::String="N",
    lng_deg::String="000", lng_min::String="00", lng_sec::String="00", lng_hem::String="W",
    city::String="   ", location_subctry::String="00",
)::String
    buf = repeat(' ', 176)
    buf = collect(buf)
    # country: 1-2
    for (i, c) in enumerate(country); i <= 2 && (buf[i] = c); end
    # state: 5-6
    for (i, c) in enumerate(state); i <= 2 && (buf[4+i] = c); end
    # airport: 7-9
    for (i, c) in enumerate(airport); i <= 3 && (buf[6+i] = c); end
    # utc_var: 11-15
    for (i, c) in enumerate(utc_var); i <= 5 && (buf[10+i] = c); end
    # lat degrees: 140-141
    for (i, c) in enumerate(lat_deg); i <= 2 && (buf[139+i] = c); end
    # lat minutes: 143-144
    for (i, c) in enumerate(lat_min); i <= 2 && (buf[142+i] = c); end
    # lat seconds: 146-147
    for (i, c) in enumerate(lat_sec); i <= 2 && (buf[145+i] = c); end
    # lat hemisphere: 148
    buf[148] = lat_hem[1]
    # lng degrees: 149-151
    for (i, c) in enumerate(lng_deg); i <= 3 && (buf[148+i] = c); end
    # lng minutes: 153-154
    for (i, c) in enumerate(lng_min); i <= 2 && (buf[152+i] = c); end
    # lng seconds: 156-157
    for (i, c) in enumerate(lng_sec); i <= 2 && (buf[155+i] = c); end
    # lng hemisphere: 158
    buf[158] = lng_hem[1]
    # metro_area: 159-161
    for (i, c) in enumerate(city); i <= 3 && (buf[158+i] = c); end
    # location_subctry: 167-168
    for (i, c) in enumerate(location_subctry); i <= 2 && (buf[166+i] = c); end
    String(buf)
end

function make_test_airports()::String
    # ORD: Chicago O'Hare — 41°58'28"N 087°54'26"W, UTC-0500
    ord = _make_airport_line(
        country="US", state="IL", airport="ORD", utc_var="-0500",
        lat_deg="41", lat_min="58", lat_sec="28", lat_hem="N",
        lng_deg="087", lng_min="54", lng_sec="26", lng_hem="W",
        city="CHI",
    )
    # LHR: Heathrow — 51°28'39"N 000°27'41"W, UTC+0000
    lhr = _make_airport_line(
        country="GB", state="00", airport="LHR", utc_var="+0000",
        lat_deg="51", lat_min="28", lat_sec="39", lat_hem="N",
        lng_deg="000", lng_min="27", lng_sec="41", lng_hem="W",
        city="LON",
    )
    # JFK: John F Kennedy — 40°38'29"N 073°46'41"W, UTC-0500
    jfk = _make_airport_line(
        country="US", state="NY", airport="JFK", utc_var="-0500",
        lat_deg="40", lat_min="38", lat_sec="29", lat_hem="N",
        lng_deg="073", lng_min="46", lng_sec="41", lng_hem="W",
        city="NYC",
    )
    join([ord, lhr, jfk], "\n") * "\n"
end

# Build a fixed-width aircraft record (102 bytes) matching the aircraft schema.
function _make_aircraft_line(; equip::String, bodytype::String=" ", description::String="")::String
    buf = repeat(' ', 102)
    buf = collect(buf)
    # equip: 7-9
    for (i, c) in enumerate(equip); i <= 3 && (buf[6+i] = c); end
    # description: 12-51
    for (i, c) in enumerate(description); i <= 40 && (buf[11+i] = c); end
    # bodytype: 60
    buf[60] = bodytype[1]
    String(buf)
end

function make_test_aircrafts()::String
    a789 = _make_aircraft_line(equip="789", bodytype="W", description="Boeing 787-9 Dreamliner")
    a320 = _make_aircraft_line(equip="320", bodytype="N", description="Airbus A320")
    join([a789, a320], "\n") * "\n"
end
