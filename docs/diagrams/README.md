# Architecture Diagrams

Drawio (diagrams.net) files documenting the architecture and workflows of
`ItinerarySearch.jl`.

## Files

| File | What it shows |
|---|---|
| [`architecture-modules.drawio`](architecture-modules.drawio) | The `src/` subsystem layout (types, ingest, store, graph, audit, observe, output, ext) and their dependency direction. Arrows point from dependent to dependency. |
| [`ingest-pipelines.drawio`](ingest-pipelines.drawio) | The two ingest paths — SSIM fixed-width and NewSSIM CSV — converging on the same `build_graph!` / `build_connections!` output. |
| [`search-workflow.drawio`](search-workflow.drawio) | How a search request becomes itineraries: inputs → `search_itineraries` (DFS + two rule chains + `_validate_and_commit!`) → output formats → observability hooks. |
| [`entry-points.drawio`](entry-points.drawio) | CLI (`bin/itinsearch.jl`, 6 commands) and REST API (`src/server.jl`, 5 endpoints) as thin shells over a shared core pipeline. |
| [`data-model.drawio`](data-model.drawio) | Type relationships across five layers: leaf aliases/enums → record types → graph types → search outputs → config & constraints. |

## Opening & editing

- **VS Code:** install the *Draw.io Integration* extension; `.drawio` files open as a visual editor.
- **Desktop:** [Drawio Desktop](https://www.drawio.com/) opens `.drawio` files natively.
- **Browser:** open [app.diagrams.net](https://app.diagrams.net), then `File → Open from → Device`.

## Conventions

- **Rounded rectangles** = subsystem containers or compute steps.
- **Cylinders** = DuckDB tables.
- **Document icons** = external input files.
- **Colours:** blue = entry points / DuckDB containers, green = Julia compute, yellow = output artefacts / DB tables, purple = inputs / config, orange = search results.
- **Solid arrows** = contains / produces. **Dashed arrows** = references / wraps / non-data hooks.

## Regenerating SVG/PNG exports

From the command line, if you have Drawio CLI installed:

```sh
drawio -x -f svg -o architecture-modules.svg architecture-modules.drawio
```

SVG/PNG exports are not checked in; regenerate on demand.
