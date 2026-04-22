# Configuration Reference

Prose companion to [`config/defaults.json`](defaults.json). The JSON file is an exhaustive, valid-JSON exemplar showing every `SearchConfig` field at its compiled-in default; this document explains what each field does, which rules or subsystems consume it, and when you'd want to tune it.

## How configs are loaded

```julia
cfg = load_config("config/defaults.json")   # from file
cfg = SearchConfig(Dict(:max_stops => 3))    # from a dict
cfg = SearchConfig(max_stops=3)              # kwargs
```

All three forms converge on the same immutable `SearchConfig` struct. `load_config` silently falls back to the compiled-in default for any key that's absent or wrongly-typed. Unknown keys in the `Dict` form throw `ArgumentError`; unknown keys in the JSON form are ignored (forward-compatibility).

**Rule of thumb**: JSON files are *exemplars*, dict constructors are *programmatic*, kwargs are *Julia-native*. Pick the one that matches the caller.

## Field groups

The JSON schema mirrors the logical grouping below — one section per topic, flat scalars within each.

### `store` — database backend

| Key | Default | Description |
|---|---|---|
| `backend` | `"duckdb"` | Currently the only supported value. Reserved so a Postgres/Aurora/Parquet-lake backend can be switched in without breaking existing configs. |
| `path` | `":memory:"` | DuckDB database path. `":memory:"` is an in-process ephemeral database (fastest, nothing persisted). File paths produce an on-disk DuckDB file that survives across runs — useful for skipping re-ingest when the schedule data hasn't changed. |

### `data` — input file paths

All paths are resolved relative to the current working directory (or absolute if given). For library use, callers usually override these to point at their own `data/input/` layout.

| Key | Default | Description |
|---|---|---|
| `ssim` | `data/input/uaoa_ssim.new.dat` | OAG/SSIM fixed-width schedule file. Type 1-5 records. |
| `mct` | `data/input/MCTIMFILUA.DAT` | MCT (Minimum Connecting Time) reference file. |
| `airports` | `data/input/mdstua.txt` | Airport master file in MDSTUA format (codes, coordinates, country, metro). |
| `regions` | `data/input/REGIMFILUA.DAT` | Region-to-airport mapping (IATA region codes, Schengen flags). |
| `aircrafts` | `data/input/aircraft.txt` | Aircraft type reference (code → wide/narrow body classification). |
| `oa_control` | `data/input/oa_control_table.csv` | Operating-airline control table for codeshare resolution. |
| `seats` | `data/input/seats_ua.txt.DAT` | **RESERVED** — seat-capacity data. Parsed on load but not yet consumed by any subsystem. |
| `classmap` | `data/input/classmaptable.txt` | **RESERVED** — booking class mapping. Not yet consumed. |
| `serviceclass` | `data/input/servclasstable.dat` | **RESERVED** — service-class codes. Not yet consumed. |
| `constraints` | `data/output` | Base path for emitted constraint files (audit output, override exports). |

### `schedule` — graph-build window

The flight graph materializes legs for a window around a target date. Enlarge these to catch long-leading flights that depart before the target day but arrive on it.

| Key | Default | Description |
|---|---|---|
| `leading_days` | `1` | Days before target date included in the schedule window. |
| `trailing_days` | `1` | Days after target date included. |
| `max_days` | `2` | Maximum schedule span for multi-date runs (used by CLI fan-out). |

### `search` — itinerary-search semantics

The tunable knobs for what counts as a valid connection or itinerary.

| Key | Default | Description |
|---|---|---|
| `max_stops` | `2` | Maximum intermediate stops. `0` = nonstops only; `1` = up to 1-stop; `2` = up to 2-stop. Most commonly overridden field. |
| `max_connection_minutes` | `480` | Upper bound on any single connection (minutes). |
| `max_elapsed_minutes` | `1440` | Upper bound on total itinerary elapsed time (minutes). |
| `circuity_factor` | `2.5` | Max ratio of flown distance to great-circle distance. Itineraries exceeding this are rejected as too circuitous. |
| `circuity_extra_miles` | `500.0` | Flat mileage tolerance added on top of `circuity_factor × great-circle`. Important for short-haul where the proportional factor leaves no slack. |
| `scope` | `"all"` | Scope filter. `"all"` = no filter, `"dom"` = domestic only, `"intl"` = international only. |
| `interline` | `"codeshare"` | Interline policy. `"online"` = same marketing carrier throughout; `"codeshare"` = same marketing carrier allows different operating carriers; `"all"` = any carrier combination (international-only unless domestic interline is specifically allowed). |
| `allow_roundtrips` | `false` | When `false`, itineraries whose final destination equals origin are rejected. When `true`, they're split at the farthest-from-origin point and committed as two halves. |
| `distance_formula` | `"haversine"` | Great-circle formula. `"haversine"` (fast, assumes spherical Earth) or `"vincenty"` (slower, ellipsoidal, sub-0.5% accuracy improvement on long-haul). |
| `maft_enabled` | `true` | Enables the MAFT (Maximum Allowable Flying Time) rule, which caps total block time against a distance-based formula with a per-stop allowance. |
| `interline_dcnx_enabled` | `true` | Enables the interline-double-connect restriction — blocks itineraries with two interline connections in a row. |
| `crs_cnx_enabled` | `true` | Enables the CRS distance-based max-connection-time rule (stricter than `max_connection_minutes` for short-haul pairs). |

### `mct_behaviour` — MCT lookup behaviour

The MCT (Minimum Connecting Time) cascade is stateful and has several tiebreaker-style toggles. These live in their own section because they rarely need to be tuned together with `search` semantics, and grouping them makes "MCT-related config" discoverable as one unit.

| Key | Default | Description |
|---|---|---|
| `mct_cache_enabled` | `true` | Cache MCT lookup results during connection build. 77% cache hit rate on full schedule; disable only when debugging MCT logic. |
| `mct_serial_ascending` | `false` | Tiebreaker direction when multiple MCT records match. `false` = higher record serial wins (later record, matches production); `true` = lower serial wins (earlier record). |
| `mct_codeshare_mode` | `"both"` | Which carrier field is used in MCT lookup. `"both"` (default, query marketing and operating), `"marketing"`, or `"operating"`. |
| `mct_schengen_mode` | `"sch_then_eur"` | Schengen/EUR region priority when both could apply. `"sch_then_eur"` (default), `"eur_then_sch"`, `"sch_only"`, `"eur_only"`. |
| `mct_suppressions_enabled` | `true` | Include MCT suppression records during lookup. `false` = ignore all suppressions (testing/debug). |

**Backward compat note**: `mct_cache_enabled` can also be set under `search` for backward compatibility with older configs. When both locations are set, `mct_behaviour.mct_cache_enabled` takes precedence.

### `graph` — graph export/import paths

| Key | Default | Description |
|---|---|---|
| `export_path` | `"data/output"` | Base directory for graph snapshots (planned — graph export is not yet wired to a file format). |
| `import_path` | `"data/output"` | Base directory for graph imports (symmetric to export_path). |

### `output` — observability and emission

| Key | Default | Description |
|---|---|---|
| `metrics_level` | `"full"` | Instrumentation depth. `"basic"` = counters only; `"aircraft"` = + aircraft-type histograms; `"full"` = + MCT audit rows and per-connection detail. |
| `event_log_enabled` | `false` | Enable structured JSONL event log (phases, system metrics, build snapshots). |
| `event_log_path` | `"data/output/events.jsonl"` | Path when `event_log_enabled = true`. |
| `log_level` | `"info"` | Standard log level. `"debug"`, `"info"`, `"warn"`, `"error"`. |
| `log_json_path` | `""` | When non-empty, emit structured JSON logs to this path (DynaTrace-compatible format). |
| `log_stdout_json` | `false` | When `true`, also mirror structured JSON logs to stdout. |
| `output_formats` | `["json", "yaml", "csv"]` | Default output formats for tooling that respects this flag. Most callers override per call. |

### `mct_audit` — MCT Inspector audit trail

Controls the detailed per-connection MCT audit log written during graph build. Disabled by default (has real overhead on full schedules).

| Key | Default | Description |
|---|---|---|
| `enabled` | `false` | Enable audit logging. |
| `detail` | `"summary"` | `"summary"` = CSV with one row per connection; `"detailed"` = JSONL with full candidate cascade per connection. |
| `output_path` | `""` | File path for audit output. Empty string means stdout. |
| `max_connections` | `0` | Stop after N connections (`0` = unlimited). Useful for targeted debugging. |
| `max_candidates` | `10` | In `detailed` mode, how many top candidates to emit per cascade. |

## Common tasks

**Tune a single field without copying the whole defaults file:**

```julia
using ItinerarySearch
cfg = SearchConfig(max_stops=3)     # Julia kwargs
cfg = SearchConfig(Dict(:max_stops => 3))   # from a dict (e.g. env-var driven)
```

**Start from a file and patch one section:**

```julia
base = load_config("config/defaults.json")
cfg  = SearchConfig(max_stops=3, interline=INTERLINE_ALL)  # clean override
```

*(Because `SearchConfig` is immutable, you can't mutate `base` — build a new instance with the keywords you care about.)*

**Ship a deployment config that overrides only a few knobs:**

Copy `config/defaults.json`, delete the sections you don't need, and keep only the overrides. Missing keys fall back to compiled-in defaults — no need to repeat the defaults.

```json
{
  "search": {"max_stops": 3, "interline": "all"},
  "mct_behaviour": {"mct_serial_ascending": true}
}
```

## Keeping this document in sync

The test `load_config — exhaustive config/defaults.json round-trips to SearchConfig()` (in `test/test_config.jl`) asserts that `defaults.json` and `SearchConfig()` produce the same config field-by-field. When you add a new field to `SearchConfig`, the test will fail until you also add the field to `defaults.json`. That's the guardrail; this document needs to be updated manually alongside.
