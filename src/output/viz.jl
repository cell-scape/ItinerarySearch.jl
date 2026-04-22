# src/output/viz.jl — Interactive HTML visualizations for graph and search results
#
# Provides three self-contained HTML generators:
#   - viz_network_map     — Leaflet map of stations, legs, and highlighted itineraries
#   - viz_timeline        — D3 Gantt-style timeline of itinerary legs
#   - viz_trip_comparison — D3 stacked bar chart of trip scoring criteria

using JSON3

# ── Shared helpers ─────────────────────────────────────────────────────────────

"""
    `function _viz_escape_title(s::AbstractString)::String`

Escapes double-quotes in a string for safe embedding in an HTML title attribute.
"""
function _viz_escape_title(s::AbstractString)::String
    replace(s, "\"" => "&quot;")
end

"""
    `function _viz_airline_color(airline::String)::String`

Returns a deterministic dark-theme hex color for a given airline code.
Uses a small fixed palette cycled by hash so the same carrier always gets
the same color within a single page.
"""
function _viz_airline_color(airline::String)::String
    palette = [
        "#4e9af1", "#f1a14e", "#4ef17a", "#f14e7a", "#c34ef1",
        "#f1e14e", "#4ef1e1", "#f14e4e", "#7af14e", "#f14ec3",
    ]
    idx = mod1(abs(hash(airline)), length(palette))
    return palette[idx]
end

# Used by `_viz_legs_from_itinerary` to convert Julia DateTime values
# (whose internal representation is milliseconds since 0001-01-01) into
# Unix-epoch milliseconds for JavaScript Date interop.
const _UNIX_EPOCH_MS = Dates.value(DateTime(1970, 1, 1))

"""
    `function _viz_legs_from_itinerary(itn::Itinerary)::Vector{Dict{String,Any}}`

Flattens an `Itinerary` into a list of leg dicts suitable for JSON serialisation.
Each dict contains `carrier`, `flight_number`, `flt_id`, `departure_station`,
`arrival_station`, `dep_dt` / `arr_dt` (ISO-8601 UTC strings for tooltips),
`dep_unix_ms` / `arr_unix_ms` (absolute Unix-epoch milliseconds for timeline
X-axis positioning — JavaScript-friendly, sortable, multi-day correct),
`aircraft_type`, `distance`, `cnx_time`, `mct`.

Times are computed via `leg_departure_dt` / `leg_arrival_dt`, which include
the +1 day rollover inference for overnight records with blank
`arrival_date_variation`.  Because the values are absolute UTC, downstream
viz code can place legs on a shared global X axis (anchored to the earliest
first-leg departure across all itineraries) without day-wrap artifacts.
"""
function _viz_legs_from_itinerary(itn::Itinerary)::Vector{Dict{String,Any}}
    result = Dict{String,Any}[]
    isempty(itn.connections) && return result

    _iso(dt::DateTime) = string(dt) * "Z"   # mark as UTC
    _unix_ms(dt::DateTime) = Dates.value(dt) - _UNIX_EPOCH_MS

    n_cnx = length(itn.connections)
    for (i, cp) in enumerate(itn.connections)
        from_l = cp.from_leg::GraphLeg
        to_l = cp.to_leg::GraphLeg
        is_nonstop_cp = cp.from_leg === cp.to_leg

        # Emit from_leg
        r = from_l.record
        dep_dt = leg_departure_dt(from_l)
        arr_dt = leg_arrival_dt(from_l)
        push!(result, Dict{String,Any}(
            "carrier"           => strip(String(r.carrier)),
            "flight_number"     => Int(r.flight_number),
            "flt_id"            => flight_id(r),
            "departure_station" => strip(String(r.departure_station)),
            "arrival_station"   => strip(String(r.arrival_station)),
            "dep_dt"            => _iso(dep_dt),
            "arr_dt"            => _iso(arr_dt),
            "dep_unix_ms"       => _unix_ms(dep_dt),
            "arr_unix_ms"       => _unix_ms(arr_dt),
            "aircraft_type"     => strip(String(r.aircraft_type)),
            "distance"          => round(Float64(from_l.distance); digits=0),
            "cnx_time"          => i > 1 ? Int(cp.cnx_time) : 0,
            "mct"               => i > 1 ? Int(cp.mct) : 0,
            "is_cnx"            => false,
        ))

        # For the final connecting leg, emit to_leg as well
        if i == n_cnx && !is_nonstop_cp
            tr = to_l.record
            t_dep_dt = leg_departure_dt(to_l)
            t_arr_dt = leg_arrival_dt(to_l)
            push!(result, Dict{String,Any}(
                "carrier"           => strip(String(tr.carrier)),
                "flight_number"     => Int(tr.flight_number),
                "flt_id"            => flight_id(tr),
                "departure_station" => strip(String(tr.departure_station)),
                "arrival_station"   => strip(String(tr.arrival_station)),
                "dep_dt"            => _iso(t_dep_dt),
                "arr_dt"            => _iso(t_arr_dt),
                "dep_unix_ms"       => _unix_ms(t_dep_dt),
                "arr_unix_ms"       => _unix_ms(t_arr_dt),
                "aircraft_type"     => strip(String(tr.aircraft_type)),
                "distance"          => round(Float64(to_l.distance); digits=0),
                "cnx_time"          => Int(cp.cnx_time),
                "mct"               => Int(cp.mct),
                "is_cnx"            => false,
            ))
        end
    end
    return result
end

# ── viz_network_map ────────────────────────────────────────────────────────────

"""
    `function viz_network_map(path::AbstractString, graph::FlightGraph, date::Date; itineraries=Itinerary[], map_mode=:leaflet, title="")`
---

# Description
- Generates a self-contained interactive HTML map of the flight network for a
  specific operating date
- Stations are rendered as circle markers sized by departure count
- Legs are drawn as thin arcs; highlighted itinerary legs appear as thicker
  orange/red polylines
- Uses Leaflet 1.9 with OpenStreetMap tiles (`:leaflet` mode) or a plain dark
  background (`:offline` mode)
- Output directory is created on demand

# Arguments
1. `path::AbstractString`: output file path (e.g., `"data/viz/network_2026-03-18.html"`)
2. `graph::FlightGraph`: built flight graph
3. `date::Date`: operating date — only legs valid on this date are shown

# Keyword Arguments
- `itineraries=Itinerary[]`: itineraries to highlight as thick orange/red arcs
- `map_mode=:leaflet`: `:leaflet` (OSM tiles) or `:offline` (plain background)
- `title=""`: HTML page title; defaults to "Network Map — <date>"

# Returns
- `::Nothing`

# Examples
```julia
julia> viz_network_map("data/viz/network.html", graph, Date(2026, 3, 18));
```
"""
function viz_network_map(
    path::AbstractString,
    graph::FlightGraph,
    date::Date;
    itineraries::Vector{Itinerary} = Itinerary[],
    map_mode::Symbol = :leaflet,
    title::String = "",
)::Nothing
    page_title = isempty(title) ? "Network Map — $(date)" : title

    # ── Collect station data ──────────────────────────────────────────────────
    stations_data = Dict{String,Any}[]
    for (code, stn) in graph.stations
        lat = stn.record.latitude
        lng = stn.record.longitude
        (lat == 0.0 && lng == 0.0) && continue
        push!(stations_data, Dict{String,Any}(
            "code"       => String(code),
            "lat"        => lat,
            "lng"        => lng,
            "departures" => length(stn.departures),
            "arrivals"   => length(stn.arrivals),
            "country"    => strip(String(stn.country)),
        ))
    end

    # ── Collect leg arcs for the operating date ───────────────────────────────
    legs_data = Dict{String,Any}[]
    for leg in graph.legs
        _operates_on(leg.record, date) || continue
        org_stn = leg.org
        dst_stn = leg.dst
        (org_stn isa GraphStation && dst_stn isa GraphStation) || continue
        org_lat = org_stn.record.latitude
        org_lng = org_stn.record.longitude
        dst_lat = dst_stn.record.latitude
        dst_lng = dst_stn.record.longitude
        (org_lat == 0.0 && org_lng == 0.0) && continue
        (dst_lat == 0.0 && dst_lng == 0.0) && continue
        push!(legs_data, Dict{String,Any}(
            "departure_station" => strip(String(leg.record.departure_station)),
            "arrival_station"   => strip(String(leg.record.arrival_station)),
            "org_lat" => org_lat,
            "org_lng" => org_lng,
            "dst_lat" => dst_lat,
            "dst_lng" => dst_lng,
            "carrier"        => strip(String(leg.record.carrier)),
            "flight_number"  => Int(leg.record.flight_number),
            "flt_id"         => flight_id(leg.record),
            "dist"           => round(Float64(leg.distance); digits=0),
            "intl"    => is_international(leg.record.dep_intl_dom == 'I' ||
                         leg.record.arr_intl_dom == 'I' ?
                         STATUS_INTERNATIONAL : StatusBits(0)),
        ))
    end

    # ── Collect highlighted itinerary arcs ────────────────────────────────────
    hilight_data = Dict{String,Any}[]
    for (itn_idx, itn) in enumerate(itineraries)
        itn_legs = _viz_legs_from_itinerary(itn)
        # Walk connections to get org/dst station coords
        n_cnx = length(itn.connections)
        for (i, cp) in enumerate(itn.connections)
            from_l = cp.from_leg::GraphLeg
            to_l = cp.to_leg::GraphLeg
            is_nonstop_cp = cp.from_leg === cp.to_leg

            function _arc(leg::GraphLeg)
                org_stn = leg.org
                dst_stn = leg.dst
                (org_stn isa GraphStation && dst_stn isa GraphStation) || return nothing
                Dict{String,Any}(
                    "itn_idx"           => itn_idx,
                    "flt_id"            => flight_id(leg.record),
                    "departure_station" => strip(String(leg.record.departure_station)),
                    "arrival_station"   => strip(String(leg.record.arrival_station)),
                    "org_lat"           => org_stn.record.latitude,
                    "org_lng"           => org_stn.record.longitude,
                    "dst_lat"           => dst_stn.record.latitude,
                    "dst_lng"           => dst_stn.record.longitude,
                    "carrier"           => strip(String(leg.record.carrier)),
                )
            end

            a = _arc(from_l)
            a !== nothing && push!(hilight_data, a)
            if i == n_cnx && !is_nonstop_cp
                b = _arc(to_l)
                b !== nothing && push!(hilight_data, b)
            end
        end
    end

    stations_json = JSON3.write(stations_data)
    legs_json     = JSON3.write(legs_data)
    hilight_json  = JSON3.write(hilight_data)

    tile_block = if map_mode == :offline
        """
        // Offline mode: plain dark background
        L.tileLayer('', {
            attribution: 'Offline mode'
        });
        map.getContainer().style.background = '#1a1a2e';
        """
    else
        """
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 18
        }).addTo(map);
        """
    end

    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>$(page_title)</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { margin: 0; padding: 0; height: 100%; }
    body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', sans-serif; display: flex; flex-direction: column; }
    #header { padding: 10px 16px; background: #161b22; border-bottom: 1px solid #30363d; display: flex; align-items: center; gap: 16px; }
    #header h1 { font-size: 16px; font-weight: 600; color: #f0f6fc; }
    #stats { font-size: 12px; color: #8b949e; }
    #map { flex: 1; }
    .leaflet-container { background: #0d1117; }
    .tooltip-box { background: #161b22; border: 1px solid #30363d; border-radius: 4px; padding: 6px 10px; font-size: 12px; color: #c9d1d9; line-height: 1.6; }
    .tooltip-box strong { color: #f0f6fc; }
  </style>
</head>
<body>
  <div id="header">
    <h1>$(page_title)</h1>
    <span id="stats"></span>
  </div>
  <div id="map"></div>
  <script>
    const STATIONS = $(stations_json);
    const LEGS = $(legs_json);
    const HILIGHTS = $(hilight_json);

    const map = L.map('map', { preferCanvas: true });
    $(tile_block)

    // ── Station lookup ────────────────────────────────────────────────────────
    const stnMap = {};
    STATIONS.forEach(s => { stnMap[s.code] = s; });

    // ── Leg arcs (thin gray polylines with midpoint offset for curvature) ─────
    const legGroup = L.layerGroup().addTo(map);
    LEGS.forEach(leg => {
      const midLat = (leg.org_lat + leg.dst_lat) / 2;
      const midLng = (leg.org_lng + leg.dst_lng) / 2;
      // Offset midpoint perpendicular to the arc for slight curvature
      const dLat = leg.dst_lat - leg.org_lat;
      const dLng = leg.dst_lng - leg.org_lng;
      const curveOffset = 0.15;
      const cLat = midLat - dLng * curveOffset;
      const cLng = midLng + dLat * curveOffset;

      const line = L.polyline(
        [[leg.org_lat, leg.org_lng], [cLat, cLng], [leg.dst_lat, leg.dst_lng]],
        { color: '#30363d', weight: 1, opacity: 0.6 }
      ).addTo(legGroup);

      line.bindTooltip(`<div class="tooltip-box"><strong>\${leg.flt_id}</strong><br>\${leg.departure_station} &rarr; \${leg.arrival_station}<br>\${leg.dist.toLocaleString()} mi</div>`,
        { sticky: true });
    });

    // ── Highlighted itinerary arcs ────────────────────────────────────────────
    const itnColors = ['#ff7043', '#ffa726', '#ef5350', '#ab47bc', '#26c6da'];
    const hilightGroup = L.layerGroup().addTo(map);
    HILIGHTS.forEach(arc => {
      const color = itnColors[(arc.itn_idx - 1) % itnColors.length];
      const midLat = (arc.org_lat + arc.dst_lat) / 2;
      const midLng = (arc.org_lng + arc.dst_lng) / 2;
      const dLat = arc.dst_lat - arc.org_lat;
      const dLng = arc.dst_lng - arc.org_lng;
      const curveOffset = 0.2;
      const cLat = midLat - dLng * curveOffset;
      const cLng = midLng + dLat * curveOffset;

      L.polyline(
        [[arc.org_lat, arc.org_lng], [cLat, cLng], [arc.dst_lat, arc.dst_lng]],
        { color: color, weight: 3, opacity: 0.9 }
      ).addTo(hilightGroup).bindTooltip(
        `<div class="tooltip-box"><strong>\${arc.flt_id}</strong><br>\${arc.departure_station} &rarr; \${arc.arrival_station}<br>Itinerary #\${arc.itn_idx}</div>`,
        { sticky: true }
      );
    });

    // ── Station markers ───────────────────────────────────────────────────────
    const stnGroup = L.layerGroup().addTo(map);
    const bounds = [];
    STATIONS.forEach(s => {
      const r = Math.max(3, Math.log(s.departures + 1) * 3);
      const marker = L.circleMarker([s.lat, s.lng], {
        radius: r,
        color: '#388bfd',
        fillColor: '#1f6feb',
        fillOpacity: 0.85,
        weight: 1
      }).addTo(stnGroup);

      marker.bindTooltip(
        `<div class="tooltip-box"><strong>\${s.code}</strong> (\${s.country})<br>\${s.departures} dep &bull; \${s.arrivals} arr</div>`,
        { sticky: true }
      );
      bounds.push([s.lat, s.lng]);
    });

    // ── Fit map to station bounds ─────────────────────────────────────────────
    if (bounds.length > 0) {
      map.fitBounds(bounds, { padding: [20, 20] });
    } else {
      map.setView([20, 0], 2);
    }

    // ── Stats bar ─────────────────────────────────────────────────────────────
    document.getElementById('stats').textContent =
      `\${STATIONS.length.toLocaleString()} stations · \${LEGS.length.toLocaleString()} legs · \${HILIGHTS.length > 0 ? HILIGHTS.length + ' highlighted arcs' : 'no highlights'}`;
  </script>
</body>
</html>
"""

    mkpath(dirname(abspath(path)))
    write(path, html)
    return nothing
end

# ── viz_timeline ───────────────────────────────────────────────────────────────

"""
    `function viz_timeline(path::AbstractString, itineraries::Vector{Itinerary}; title="", max_display=50)`
---

# Description
- Generates a self-contained interactive HTML Gantt-style timeline of itinerary
  legs plotted in UTC minutes
- Each itinerary is one horizontal row; legs are colored rectangles sized
  proportionally to block time; connection gaps are lighter dashed rectangles
- Uses D3 v7 for rendering
- Output directory is created on demand

# Arguments
1. `path::AbstractString`: output file path
2. `itineraries::Vector{Itinerary}`: search results to display

# Keyword Arguments
- `title=""`: page title; defaults to "Itinerary Timeline"
- `max_display=50`: maximum number of itineraries to render

# Returns
- `::Nothing`

# Examples
```julia
julia> viz_timeline("data/viz/timeline.html", itineraries);
```
"""
function viz_timeline(
    path::AbstractString,
    itineraries::Vector{Itinerary};
    title::String = "",
    max_display::Int = 50,
)::Nothing
    page_title = isempty(title) ? "Itinerary Timeline" : title
    display_itns = itineraries[1:min(max_display, length(itineraries))]

    # ── Build chart data ──────────────────────────────────────────────────────
    rows_data = Dict{String,Any}[]
    for (itn_idx, itn) in enumerate(display_itns)
        stops = Int(itn.num_stops)
        elapsed = Int(itn.elapsed_time)
        legs = _viz_legs_from_itinerary(itn)
        isempty(legs) && continue

        push!(rows_data, Dict{String,Any}(
            "id"      => itn_idx,
            "stops"   => stops,
            "elapsed" => elapsed,
            "dist"    => round(Float64(itn.total_distance); digits=0),
            "circ"    => round(Float64(itn.circuity); digits=2),
            "intl"    => is_international(itn.status),
            "legs"    => legs,
        ))
    end

    rows_json = JSON3.write(rows_data)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>$(page_title)</title>
  <script src="https://unpkg.com/d3@7.9.0/dist/d3.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', monospace; }
    #header { padding: 12px 20px; background: #161b22; border-bottom: 1px solid #30363d; }
    #header h1 { font-size: 15px; font-weight: 600; color: #f0f6fc; }
    #header p  { font-size: 12px; color: #8b949e; margin-top: 2px; }
    #chart-container { padding: 16px 20px; overflow-x: auto; }
    .row-label { font-size: 11px; fill: #8b949e; }
    .leg-rect  { cursor: pointer; }
    .leg-rect:hover { opacity: 0.85; }
    .cnx-rect  { fill: #21262d; stroke: #388bfd; stroke-dasharray: 3,2; stroke-width: 1; opacity: 0.7; }
    .axis path, .axis line { stroke: #30363d; }
    .axis text { fill: #8b949e; font-size: 10px; }
    .grid line { stroke: #21262d; stroke-opacity: 0.8; }
    #tooltip {
      position: fixed; pointer-events: none; display: none;
      background: #161b22; border: 1px solid #30363d; border-radius: 4px;
      padding: 8px 12px; font-size: 12px; color: #c9d1d9; line-height: 1.7;
      max-width: 260px; z-index: 100;
    }
    #tooltip strong { color: #f0f6fc; }
  </style>
</head>
<body>
  <div id="header">
    <h1>$(page_title)</h1>
    <p id="subtitle"></p>
  </div>
  <div id="chart-container"><svg id="chart"></svg></div>
  <div id="tooltip"></div>
  <script>
    const ROWS = $(rows_json);

    const ROW_H   = 36;
    const ROW_PAD = 6;
    const LABEL_W = 90;
    const MARGIN  = { top: 30, right: 20, bottom: 40, left: LABEL_W };
    const MIN_LEG_W = 4;

    const container = document.getElementById('chart-container');
    const tooltip   = document.getElementById('tooltip');
    document.getElementById('subtitle').textContent =
      `\${ROWS.length} itineraries`;

    if (ROWS.length === 0) {
      document.getElementById('subtitle').textContent = 'No itineraries to display';
    } else {
      // ── Compute global time range (absolute UTC, shared across rows)
      // X axis spans from the earliest first-leg departure to the latest
      // last-leg arrival across every itinerary in view.  Each leg carries
      // dep_unix_ms / arr_unix_ms (Unix epoch milliseconds) — JavaScript
      // Date constructor accepts these directly, and D3's scaleTime
      // handles tick formatting (HH:MM, day labels for multi-day spans).
      let globalMin = Infinity, globalMax = -Infinity;
      ROWS.forEach(row => {
        if (row.legs.length === 0) return;
        const first = row.legs[0];
        const last  = row.legs[row.legs.length - 1];
        if (first.dep_unix_ms < globalMin) globalMin = first.dep_unix_ms;
        if (last.arr_unix_ms  > globalMax) globalMax = last.arr_unix_ms;
      });
      const span = globalMax - globalMin;

      const svgW = Math.max(800, container.clientWidth - 40);
      const innerW = svgW - MARGIN.left - MARGIN.right;
      const innerH = ROWS.length * (ROW_H + ROW_PAD);
      const svgH = innerH + MARGIN.top + MARGIN.bottom;

      const svg = d3.select('#chart')
        .attr('width', svgW)
        .attr('height', svgH);

      const g = svg.append('g')
        .attr('transform', `translate(\${MARGIN.left},\${MARGIN.top})`);

      const xScale = d3.scaleTime()
        .domain([new Date(globalMin), new Date(globalMax)])
        .range([0, innerW]);

      // ── X axis with absolute UTC time labels ───────────────────────────────
      // D3's default time formatter switches between "HH:MM", "DD Mon",
      // "Mon YYYY", etc. based on the tick interval.  When the chart spans
      // a single day the ticks are HH:MM; multi-day spans get day boundaries.
      const xAxis = d3.axisBottom(xScale).ticks(10);

      g.append('g')
        .attr('class', 'axis')
        .attr('transform', `translate(0,\${innerH})`)
        .call(xAxis);

      // ── Grid lines ────────────────────────────────────────────────────────
      g.append('g')
        .attr('class', 'grid')
        .attr('transform', `translate(0,\${innerH})`)
        .call(d3.axisBottom(xScale).ticks(10).tickSize(-innerH).tickFormat(''))
        .call(gg => gg.select('.domain').remove());

      // ── Airline color scale ───────────────────────────────────────────────
      const airlines = [...new Set(ROWS.flatMap(r => r.legs.map(l => l.carrier)))];
      const palette = ['#4e9af1','#f1a14e','#4ef17a','#f14e7a','#c34ef1',
                       '#f1e14e','#4ef1e1','#f14e4e','#7af14e','#f14ec3'];
      const colorMap = {};
      airlines.forEach((al, i) => { colorMap[al] = palette[i % palette.length]; });

      // ── Rows ──────────────────────────────────────────────────────────────
      ROWS.forEach((row, ri) => {
        const y = ri * (ROW_H + ROW_PAD);
        const rowG = g.append('g').attr('transform', `translate(0,\${y})`);

        // Row label
        rowG.append('text')
          .attr('class', 'row-label')
          .attr('x', -6)
          .attr('y', ROW_H / 2 + 4)
          .attr('text-anchor', 'end')
          .text(`#\${row.id} (\${row.stops}s)`);

        // ── Legs ────────────────────────────────────────────────────────────
        // Connection gap rectangles (drawn behind legs)
        for (let i = 1; i < row.legs.length; i++) {
          const prev = row.legs[i - 1];
          const curr = row.legs[i];
          const gx  = xScale(new Date(prev.arr_unix_ms));
          const gx2 = xScale(new Date(curr.dep_unix_ms));
          const gw  = Math.max(2, gx2 - gx);
          rowG.append('rect')
            .attr('class', 'cnx-rect')
            .attr('x', gx)
            .attr('y', 4)
            .attr('width', gw)
            .attr('height', ROW_H - 8)
            .on('mousemove', ev => showTooltip(ev,
              `<strong>Connection at \${curr.departure_station}</strong><br>` +
              `Gap: \${curr.cnx_time} min · MCT: \${curr.mct} min`))
            .on('mouseleave', hideTooltip);
        }

        // Leg rectangles
        row.legs.forEach(leg => {
          const lx = xScale(new Date(leg.dep_unix_ms));
          const lw = Math.max(MIN_LEG_W, xScale(new Date(leg.arr_unix_ms)) - lx);
          const color = colorMap[leg.carrier] || '#4e9af1';

          rowG.append('rect')
            .attr('class', 'leg-rect')
            .attr('x', lx)
            .attr('y', 2)
            .attr('width', lw)
            .attr('height', ROW_H - 4)
            .attr('fill', color)
            .attr('rx', 3)
            .on('mousemove', ev => showTooltip(ev,
              `<strong>\${leg.flt_id}</strong><br>` +
              `\${leg.departure_station} &rarr; \${leg.arrival_station}<br>` +
              `Dep: \${leg.dep_dt} · Arr: \${leg.arr_dt}<br>` +
              `\${leg.aircraft_type} · \${leg.distance.toLocaleString()} mi`))
            .on('mouseleave', hideTooltip);

          // Flight label inside rect if wide enough
          if (lw > 40) {
            rowG.append('text')
              .attr('x', lx + lw / 2)
              .attr('y', ROW_H / 2 + 4)
              .attr('text-anchor', 'middle')
              .attr('font-size', 10)
              .attr('fill', '#0d1117')
              .attr('pointer-events', 'none')
              .text(leg.flt_id);
          }
        });
      });

      function showTooltip(ev, html) {
        tooltip.innerHTML = html;
        tooltip.style.display = 'block';
        tooltip.style.left = (ev.clientX + 12) + 'px';
        tooltip.style.top  = (ev.clientY + 12) + 'px';
      }
      function hideTooltip() {
        tooltip.style.display = 'none';
      }
      // Move tooltip with mouse
      document.addEventListener('mousemove', ev => {
        if (tooltip.style.display !== 'none') {
          tooltip.style.left = (ev.clientX + 12) + 'px';
          tooltip.style.top  = (ev.clientY + 12) + 'px';
        }
      });
    }
  </script>
</body>
</html>
"""

    mkpath(dirname(abspath(path)))
    write(path, html)
    return nothing
end

# ── viz_trip_comparison ────────────────────────────────────────────────────────

"""
    `function viz_trip_comparison(path::AbstractString, trips::Vector{Trip}; weights=TripScoringWeights(), title="", top_n=10)`
---

# Description
- Generates a self-contained interactive HTML stacked horizontal bar chart
  comparing trip scoring criterion contributions
- Each bar represents one trip; segments are colored by criterion and sized by
  weighted contribution to the total score
- Sorted best (lowest score) first; only `top_n` trips are shown
- Uses D3 v7 for rendering
- Output directory is created on demand

# Arguments
1. `path::AbstractString`: output file path
2. `trips::Vector{Trip}`: scored trip results (e.g., from `search_trip`)

# Keyword Arguments
- `weights::TripScoringWeights`: scoring weights used to compute contributions
- `title=""`: page title; defaults to "Trip Comparison"
- `top_n=10`: maximum number of trips to display

# Returns
- `::Nothing`

# Examples
```julia
julia> viz_trip_comparison("data/viz/trips.html", trips; top_n=20);
```
"""
function viz_trip_comparison(
    path::AbstractString,
    trips::Vector{Trip};
    weights::TripScoringWeights = TripScoringWeights(),
    title::String = "",
    top_n::Int = 10,
)::Nothing
    page_title = isempty(title) ? "Trip Comparison" : title
    display_trips = trips[1:min(top_n, length(trips))]

    # ── Criterion definitions ─────────────────────────────────────────────────
    criteria = [
        ("stops",           "Stops",          "#f14e4e"),
        ("eqp_changes",     "Eqp Changes",    "#f1a14e"),
        ("carrier_changes", "Carrier Changes", "#f1e14e"),
        ("flt_no_changes",  "Flt# Changes",   "#4ef17a"),
        ("elapsed",         "Elapsed Time",   "#4e9af1"),
        ("block_time",      "Block Time",     "#4ef1e1"),
        ("layover",         "Layover",        "#c34ef1"),
        ("distance",        "Distance",       "#7af14e"),
        ("circuity",        "Circuity",       "#f14ec3"),
    ]

    # ── Decompose score into per-criterion weighted contributions ─────────────
    trips_data = Dict{String,Any}[]
    for (rank, trip) in enumerate(display_trips)
        # Recompute raw criterion values (mirrors score_trip logic)
        total_stops = 0
        total_eqp   = 0
        total_block = 0.0
        carrier_changes = 0
        flt_no_changes  = 0
        prev_carrier = NO_AIRLINE
        prev_flt     = FlightNumber(0)
        circ_sum     = 0.0
        circ_count   = 0

        for itn in trip.itineraries
            total_stops += Int(itn.num_stops)
            total_eqp   += Int(itn.num_eqp_changes)
            if itn.market_distance > Distance(0)
                circ_sum   += Float64(itn.circuity)
                circ_count += 1
            end
            for cp in itn.connections
                cp.from_leg === cp.to_leg && continue
                r = (cp.to_leg::GraphLeg).record
                bt = Float64(r.passenger_arrival_time) - Float64(r.passenger_departure_time) + Float64(r.arrival_date_variation) * 1440.0
                if bt < 0.0; bt += 1440.0; end
                total_block += bt
                curr_carrier = r.carrier
                curr_flt     = r.flight_number
                if prev_carrier != NO_AIRLINE
                    curr_carrier != prev_carrier && (carrier_changes += 1)
                    curr_flt != prev_flt         && (flt_no_changes  += 1)
                end
                prev_carrier = curr_carrier
                prev_flt     = curr_flt
            end
        end

        total_block   /= 60.0
        total_elapsed  = Float64(trip.total_elapsed) / 60.0
        total_layover  = max(0.0, total_elapsed - total_block)
        total_dist     = Float64(trip.total_distance) / 1000.0
        avg_circ       = circ_count > 0 ? circ_sum / circ_count : 1.0

        raw = Dict(
            "stops"           => Float64(total_stops),
            "eqp_changes"     => Float64(total_eqp),
            "carrier_changes" => Float64(carrier_changes),
            "flt_no_changes"  => Float64(flt_no_changes),
            "elapsed"         => total_elapsed,
            "block_time"      => total_block,
            "layover"         => total_layover,
            "distance"        => total_dist,
            "circuity"        => max(0.0, avg_circ - 1.0),
        )
        w_dict = Dict(
            "stops"           => weights.stops,
            "eqp_changes"     => weights.eqp_changes,
            "carrier_changes" => weights.carrier_changes,
            "flt_no_changes"  => weights.flt_no_changes,
            "elapsed"         => weights.elapsed,
            "block_time"      => weights.block_time,
            "layover"         => weights.layover,
            "distance"        => weights.distance,
            "circuity"        => weights.circuity,
        )

        contributions = Dict{String,Any}[]
        for (key, label, color) in criteria
            rv = raw[key]
            wv = w_dict[key]
            push!(contributions, Dict{String,Any}(
                "key"       => key,
                "label"     => label,
                "color"     => color,
                "raw"       => round(rv; digits=2),
                "weight"    => wv,
                "weighted"  => round(rv * wv; digits=3),
            ))
        end

        # Route string
        route = String(trip.origin) * " → " * String(trip.destination)

        push!(trips_data, Dict{String,Any}(
            "rank"          => rank,
            "trip_id"       => Int(trip.trip_id),
            "route"         => route,
            "trip_type"     => String(trip.trip_type),
            "score"         => round(trip.score; digits=3),
            "total_elapsed" => Int(trip.total_elapsed),
            "total_dist"    => round(Float64(trip.total_distance); digits=0),
            "n_itineraries" => length(trip.itineraries),
            "contributions" => contributions,
        ))
    end

    criteria_json = JSON3.write([(k, l, c) for (k, l, c) in criteria])
    trips_json    = JSON3.write(trips_data)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>$(page_title)</title>
  <script src="https://unpkg.com/d3@7.9.0/dist/d3.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', sans-serif; }
    #header { padding: 12px 20px; background: #161b22; border-bottom: 1px solid #30363d; }
    #header h1 { font-size: 15px; font-weight: 600; color: #f0f6fc; }
    #header p  { font-size: 12px; color: #8b949e; margin-top: 2px; }
    #legend { display: flex; flex-wrap: wrap; gap: 8px; padding: 12px 20px; background: #161b22; border-bottom: 1px solid #30363d; }
    .legend-item { display: flex; align-items: center; gap: 5px; font-size: 11px; color: #8b949e; }
    .legend-swatch { width: 12px; height: 12px; border-radius: 2px; flex-shrink: 0; }
    #chart-container { padding: 16px 20px; overflow-x: auto; }
    .bar-label { font-size: 11px; fill: #8b949e; cursor: default; }
    .bar-label:hover { fill: #f0f6fc; }
    .axis path, .axis line { stroke: #30363d; }
    .axis text { fill: #8b949e; font-size: 10px; }
    .grid line { stroke: #21262d; stroke-opacity: 0.8; }
    .score-text { font-size: 10px; fill: #8b949e; }
    #tooltip {
      position: fixed; pointer-events: none; display: none;
      background: #161b22; border: 1px solid #30363d; border-radius: 4px;
      padding: 8px 12px; font-size: 12px; color: #c9d1d9; line-height: 1.7;
      max-width: 280px; z-index: 100;
    }
    #tooltip strong { color: #f0f6fc; }
    #tooltip .dim  { color: #8b949e; font-size: 11px; }
  </style>
</head>
<body>
  <div id="header">
    <h1>$(page_title)</h1>
    <p id="subtitle"></p>
  </div>
  <div id="legend"></div>
  <div id="chart-container"><svg id="chart"></svg></div>
  <div id="tooltip"></div>
  <script>
    const CRITERIA = $(criteria_json);
    const TRIPS    = $(trips_json);

    const tooltip = document.getElementById('tooltip');
    document.getElementById('subtitle').textContent =
      `\${TRIPS.length} trip\${TRIPS.length !== 1 ? 's' : ''} · lower score = better`;

    // ── Legend ────────────────────────────────────────────────────────────────
    const legendEl = document.getElementById('legend');
    CRITERIA.forEach(([key, label, color]) => {
      const item = document.createElement('div');
      item.className = 'legend-item';
      item.innerHTML = `<div class="legend-swatch" style="background:\${color}"></div>\${label}`;
      legendEl.appendChild(item);
    });

    if (TRIPS.length === 0) {
      document.getElementById('subtitle').textContent = 'No trips to display';
    } else {
      const BAR_H   = 28;
      const BAR_PAD = 10;
      const LABEL_W = 140;
      const SCORE_W = 60;
      const MARGIN  = { top: 20, right: SCORE_W + 20, bottom: 40, left: LABEL_W };

      const container = document.getElementById('chart-container');
      const svgW    = Math.max(800, container.clientWidth - 40);
      const innerW  = svgW - MARGIN.left - MARGIN.right;
      const innerH  = TRIPS.length * (BAR_H + BAR_PAD);
      const svgH    = innerH + MARGIN.top + MARGIN.bottom;

      const svg = d3.select('#chart')
        .attr('width', svgW)
        .attr('height', svgH);

      const g = svg.append('g')
        .attr('transform', `translate(\${MARGIN.left},\${MARGIN.top})`);

      // ── X scale based on max total score ────────────────────────────────
      const maxScore = d3.max(TRIPS, t => t.score);
      const xScale = d3.scaleLinear().domain([0, maxScore * 1.05]).range([0, innerW]);

      const xAxis = d3.axisBottom(xScale).ticks(8);
      g.append('g')
        .attr('class', 'axis')
        .attr('transform', `translate(0,\${innerH})`)
        .call(xAxis);

      g.append('g')
        .attr('class', 'grid')
        .attr('transform', `translate(0,\${innerH})`)
        .call(d3.axisBottom(xScale).ticks(8).tickSize(-innerH).tickFormat(''))
        .call(gg => gg.select('.domain').remove());

      g.append('text')
        .attr('x', innerW / 2)
        .attr('y', innerH + 36)
        .attr('text-anchor', 'middle')
        .attr('font-size', 11)
        .attr('fill', '#8b949e')
        .text('Weighted Score (lower = better)');

      // ── Trip bars ─────────────────────────────────────────────────────────
      TRIPS.forEach((trip, ti) => {
        const y = ti * (BAR_H + BAR_PAD);
        const rowG = g.append('g').attr('transform', `translate(0,\${y})`);

        // Label
        const labelText = `#\${trip.rank} \${trip.route}`;
        rowG.append('text')
          .attr('class', 'bar-label')
          .attr('x', -8)
          .attr('y', BAR_H / 2 + 4)
          .attr('text-anchor', 'end')
          .text(labelText)
          .on('mousemove', ev => showTooltip(ev,
            `<strong>\${trip.route}</strong><br>` +
            `<span class="dim">Type: \${trip.trip_type} · ID: \${trip.trip_id}</span><br>` +
            `Score: \${trip.score}<br>` +
            `Elapsed: \${trip.total_elapsed} min · Dist: \${trip.total_dist.toLocaleString()} mi`))
          .on('mouseleave', hideTooltip);

        // Stacked segments
        let cursor = 0;
        trip.contributions.forEach(c => {
          if (c.weighted <= 0) return;
          const segW = xScale(c.weighted);
          rowG.append('rect')
            .attr('x', cursor)
            .attr('y', 2)
            .attr('width', segW)
            .attr('height', BAR_H - 4)
            .attr('fill', c.color)
            .attr('rx', 2)
            .on('mousemove', ev => showTooltip(ev,
              `<strong>\${c.label}</strong><br>` +
              `Raw: \${c.raw}<br>` +
              `Weight: \${c.weight}<br>` +
              `Contribution: \${c.weighted}`))
            .on('mouseleave', hideTooltip);
          cursor += segW;
        });

        // Score label at end of bar
        rowG.append('text')
          .attr('class', 'score-text')
          .attr('x', xScale(trip.score) + 4)
          .attr('y', BAR_H / 2 + 4)
          .text(trip.score.toFixed(1));
      });

      function showTooltip(ev, html) {
        tooltip.innerHTML = html;
        tooltip.style.display = 'block';
        tooltip.style.left = (ev.clientX + 12) + 'px';
        tooltip.style.top  = (ev.clientY + 12) + 'px';
      }
      function hideTooltip() { tooltip.style.display = 'none'; }
      document.addEventListener('mousemove', ev => {
        if (tooltip.style.display !== 'none') {
          tooltip.style.left = (ev.clientX + 12) + 'px';
          tooltip.style.top  = (ev.clientY + 12) + 'px';
        }
      });
    }
  </script>
</body>
</html>
"""

    mkpath(dirname(abspath(path)))
    write(path, html)
    return nothing
end

# ── ItineraryRef Table Visualization ─────────────────────────────────────────

"""
    `function viz_itinerary_refs(path, data; title="")`

Generate a self-contained interactive HTML table of ItineraryRefs.

Accepts either:
- `Vector{ItineraryRef}` — flat list
- `Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}}` — nested dict from `itinerary_legs_multi`

Features: sortable columns, filterable by origin/destination/stops, expandable rows showing LegKeys.
"""
function viz_itinerary_refs(
    path::AbstractString,
    data::Vector{ItineraryRef};
    title::String = "",
)::Nothing
    _write_itinref_html(path, _itinrefs_to_json(data), title)
end

function viz_itinerary_refs(
    path::AbstractString,
    data::Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}};
    title::String = "",
)::Nothing
    entries = Dict{String,Any}[]
    for (date, org_dict) in data
        for (org, dst_dict) in org_dict
            for (dst, itins) in dst_dict
                for (i, itn) in enumerate(itins)
                    push!(entries, _itinref_entry(itn, i; date=string(date)))
                end
            end
        end
    end
    _write_itinref_html(path, JSON3.write(entries), title)
end

# ── Rich overloads (Vector{Itinerary} input) ─────────────────────────────────
# Accept full `Itinerary` structs (with `GraphConnection` chain intact) so the
# viz can emit per-connection MCT info, per-leg TRC codes, status flags
# (codeshare/interline/through), and absolute UTC timestamps.  ItineraryRef
# overloads stay above for callers that only have the compact form.

function viz_itinerary_refs(
    path::AbstractString,
    data::Vector{Itinerary};
    title::String = "",
    date::String = "",
    graph::Union{FlightGraph,Nothing} = nothing,
)::Nothing
    deduped = _dedup_visible(data)
    dei10_filter = graph === nothing ? nothing : _build_valid_flight_set(graph)
    entries = Dict{String,Any}[
        _itinref_entry_rich(itn, i; date=date, dei10_filter=dei10_filter)
        for (i, itn) in enumerate(deduped)
    ]
    _write_itinref_html(path, JSON3.write(entries), title)
end

function viz_itinerary_refs(
    path::AbstractString,
    data::Dict{Date, Dict{String, Dict{String, Vector{Itinerary}}}};
    title::String = "",
    graph::Union{FlightGraph,Nothing} = nothing,
)::Nothing
    dei10_filter = graph === nothing ? nothing : _build_valid_flight_set(graph)
    entries = Dict{String,Any}[]
    for (d, org_dict) in data
        for (_, dst_dict) in org_dict
            for (_, itins) in dst_dict
                deduped = _dedup_visible(itins)
                for (i, itn) in enumerate(deduped)
                    push!(entries, _itinref_entry_rich(
                        itn, i; date=string(d), dei10_filter=dei10_filter,
                    ))
                end
            end
        end
    end
    _write_itinref_html(path, JSON3.write(entries), title)
end

# ── DEI 10 cross-reference filter ─────────────────────────────────────────────
# DEI 10 is a slash-separated list of marketing aliases on a host flight,
# e.g. "AC 4893 /NZ 9070 /VA 8355".  Many of those listed dupes are not
# present in our schedule (foreign-carrier inventory, GDS-only listings,
# etc.); showing them as "also marketed as ..." is misleading because the
# user can't actually book them via this graph.  When a `graph` is passed
# to viz_itinerary_refs, we build a set of valid (carrier, flight_number)
# pairs from `graph.legs` and filter each leg's dei_10 against it.

function _build_valid_flight_set(graph::FlightGraph)::Set{Tuple{String,Int16}}
    s = Set{Tuple{String,Int16}}()
    for leg in graph.legs
        push!(s, (strip(String(leg.record.carrier)), leg.record.flight_number))
    end
    return s
end

function _filter_dei10(dei10::AbstractString,
                      filter::Union{Set{Tuple{String,Int16}},Nothing})::String
    isempty(dei10) && return ""
    filter === nothing && return String(dei10)   # no filter = pass through
    parts = split(dei10, '/')
    kept = String[]
    for p in parts
        toks = split(strip(p))
        length(toks) >= 2 || continue
        c = String(toks[1])
        fno = tryparse(Int, String(toks[2]))
        fno === nothing && continue
        if (c, Int16(fno)) in filter
            push!(kept, "$c $fno")
        end
    end
    return join(kept, " / ")
end

# ── Viz-only dedup ────────────────────────────────────────────────────────────
# `_itinerary_fingerprint` (in formats.jl) hashes by `row_number` to detect
# leg-identical itineraries.  But SSIM occasionally has multiple Type-3
# records describing the same logical flight (overlapping eff/disc date
# ranges, different carrier suffixes, etc.) — different row_numbers, same
# user-visible fields.  The DataFrame / JSON paths keep these as distinct
# data points (callers may want them for analysis); the viz collapses
# them so the table doesn't show consecutive identical-looking rows.
#
# Visible fingerprint: per-leg (carrier, flight_number, operating_date,
# departure_station, arrival_station, leg_sequence_number).  Same operating
# date + same flight identity at the leg level = same row in the viz.

function _itinerary_visible_fingerprint(itn::Itinerary)::UInt64
    # Mirrors the structure of `_itinerary_fingerprint` (formats.jl) — explicit
    # check for `from_l === to_l` (the nonstop self-cp case) avoids the
    # `for leg in (from_l, to_l)` pitfall where both iterations would skip
    # the same leg and yield h=0 for every nonstop.
    h = UInt64(0)
    last_rn = UInt64(0)
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        to_l   = cp.to_leg::GraphLeg
        rn = from_l.record.row_number
        if rn != last_rn
            r = from_l.record
            h = hash((r.carrier, r.flight_number, r.operating_date,
                      r.departure_station, r.arrival_station,
                      r.leg_sequence_number), h)
            last_rn = rn
        end
        if !(from_l === to_l)
            rn2 = to_l.record.row_number
            if rn2 != last_rn
                r2 = to_l.record
                h = hash((r2.carrier, r2.flight_number, r2.operating_date,
                          r2.departure_station, r2.arrival_station,
                          r2.leg_sequence_number), h)
                last_rn = rn2
            end
        end
    end
    return h
end

function _dedup_visible(itns::Vector{Itinerary})::Vector{Itinerary}
    seen = Set{UInt64}()
    out  = Itinerary[]
    for itn in itns
        fp = _itinerary_visible_fingerprint(itn)
        fp in seen && continue
        push!(seen, fp)
        push!(out, itn)
    end
    return out
end

# ── Helpers ──────────────────────────────────────────────────────────────────

# Map MCTSource enum to a short human-readable string for the viz tooltip /
# expanded row.  Uses the existing canonical names from src/types/enums.jl.
function _mct_source_str(src::MCTSource)::String
    src == SOURCE_EXCEPTION         && return "exception"
    src == SOURCE_STATION_STANDARD  && return "standard"
    src == SOURCE_GLOBAL_DEFAULT    && return "default"
    return string(src)
end

# Extract the TRC character that applies to this leg (matches the connection
# rule's `_get_trc` logic: indexed-by-leg-sequence string like "AB" with
# leg_sequence_number 2 → 'B'; single-char "A" → 'A'; '.' or empty → ' ').
function _leg_trc_char(rec)::Char
    trc = rec.traffic_restriction_for_leg
    (isempty(trc) || trc == InlineString15(".")) && return ' '
    length(trc) <= 1 && return trc[1]
    seq = Int(rec.leg_sequence_number)
    (seq > 0 && seq <= length(trc)) ? trc[seq] : ' '
end

function _itinref_entry_rich(itn::Itinerary, idx::Int;
                              date::String="",
                              dei10_filter::Union{Set{Tuple{String,Int16}},Nothing}=nothing)
    legs, cnxs = _extract_legs_and_cnxs(itn)
    n_legs = length(legs)

    # Per-leg dicts (all the existing fields plus timestamps + TRC).
    # Operating-carrier / -flight-number normalization: SSIM stores them as
    # empty/0 on host flights (operating == marketing), populated only via
    # DEI 50 supplements when a different carrier+flight is the marketing
    # alias of the same physical flight.  We fold the missing case into the
    # marketing values here so downstream consumers (JS template, callers
    # of the rich entry) don't need to know about the SSIM convention.
    leg_dicts = Dict{String,Any}[]
    for (k, leg) in enumerate(legs)
        rec = leg.record
        trc = _leg_trc_char(rec)
        mkt_carrier = strip(String(rec.carrier))
        mkt_flight  = Int(rec.flight_number)
        op_carrier_raw = strip(String(rec.operating_carrier))
        op_flight_raw  = Int(rec.operating_flight_number)
        op_carrier = isempty(op_carrier_raw) ? mkt_carrier : op_carrier_raw
        op_flight  = op_flight_raw == 0      ? mkt_flight  : op_flight_raw
        is_codeshare_leg = (mkt_carrier != op_carrier) || (mkt_flight != op_flight)
        push!(leg_dicts, Dict{String,Any}(
            "leg_pos"                 => k,
            "carrier"                 => mkt_carrier,
            "flight_number"           => mkt_flight,
            "departure_station"       => strip(String(rec.departure_station)),
            "arrival_station"         => strip(String(rec.arrival_station)),
            "operating_carrier"       => op_carrier,
            "operating_flight_number" => op_flight,
            "is_codeshare_leg"        => is_codeshare_leg,
            "dei_10"                  => _filter_dei10(strip(String(rec.dei_10)), dei10_filter),
            "row_number"              => Int(rec.row_number),
            "record_serial"           => Int(rec.record_serial),
            "dep_dt"                  => string(leg_departure_dt(leg)) * "Z",
            "arr_dt"                  => string(leg_arrival_dt(leg)) * "Z",
            "aircraft_type"           => strip(String(rec.aircraft_type)),
            "trc"                     => trc == ' ' ? "" : string(trc),
        ))
    end

    # Per-connection dicts (between consecutive legs).  cnxs[i] sits between
    # legs[i] and legs[i+1]; carries MCT id/time/source for audit.
    cnx_dicts = Dict{String,Any}[]
    for (k, cp) in enumerate(cnxs)
        push!(cnx_dicts, Dict{String,Any}(
            "cnx_pos"     => k,
            "station"     => strip(String((cp.from_leg::GraphLeg).record.arrival_station)),
            "cnx_time"    => Int(cp.cnx_time),
            "mct"         => Int(cp.mct),
            "mct_id"      => Int(cp.mct_result.mct_id),
            "mct_time"    => Int(cp.mct_result.time),
            "mct_source"  => _mct_source_str(cp.mct_result.source),
            "is_through"  => cp.is_through,
        ))
    end

    # Reuse existing flight-time / layover accumulators for consistency with
    # the ItineraryRef path (which is what other tooling reads).
    flight_mins = Int32(0)
    for leg in legs
        flight_mins += _leg_utc_block(leg.record)
    end
    elapsed = Int(itn.elapsed_time)
    layover = Int(max(Int32(0), elapsed - flight_mins))

    # First/last station for the row's Origin / Dest columns (uses leg list,
    # not the LegKey path, so it works on Itinerary directly).
    origin_str = n_legs > 0 ? strip(String(legs[1].record.departure_station)) : ""
    dest_str   = n_legs > 0 ? strip(String(legs[end].record.arrival_station)) : ""

    # Build "carrier flight_number" per leg, then collapse consecutive
    # identical entries (through-flight pattern: a single flight number
    # serving multiple board/off points shouldn't be repeated in the
    # display).  Result joined with " → " to match the route style.
    flight_ids = ["$(strip(String(l.record.carrier))) $(Int(l.record.flight_number))"
                  for l in legs]
    distinct_flights = String[]
    for fid in flight_ids
        (isempty(distinct_flights) || distinct_flights[end] != fid) && push!(distinct_flights, fid)
    end
    flights_str_v = join(distinct_flights, " → ")
    route_str_v = if n_legs == 0
        ""
    elseif n_legs == 1
        "$(strip(String(legs[1].record.departure_station))) → $(strip(String(legs[1].record.arrival_station)))"
    else
        join(vcat([strip(String(legs[1].record.departure_station))],
                  [strip(String(l.record.arrival_station)) for l in legs]),
             " → ")
    end

    # Per-itinerary status flags computed from the actual leg list rather
    # than from `itn.status` bits.  The internal STATUS_CODESHARE /
    # STATUS_INTERLINE bits are set per-connection by `_set_connection_status!`
    # using a different (mutually-exclusive) convention required by the
    # search rule chain (INTERLINE_ONLINE / INTERLINE_CODESHARE filter modes).
    # For the viz we want the user-facing definitions, which are independent
    # and can co-occur on the same itinerary:
    #
    #   codeshare = ANY leg has marketing carrier+flight differing from its
    #               own operating carrier+flight (per-leg property folded up)
    #   interline = the marketing carrier CHANGES between any two consecutive
    #               legs of this itinerary (per-itinerary property)
    is_codeshare_itn = any(d -> d["is_codeshare_leg"]::Bool, leg_dicts)
    is_interline_itn = false
    if length(leg_dicts) >= 2
        first_carrier = leg_dicts[1]["carrier"]
        for d in leg_dicts
            if d["carrier"] != first_carrier
                is_interline_itn = true
                break
            end
        end
    end

    Dict{String,Any}(
        "date"             => date,
        "idx"              => idx,
        "flights"          => flights_str_v,
        "route"            => route_str_v,
        "origin"           => origin_str,
        "destination"      => dest_str,
        "num_stops"        => Int(itn.num_stops),
        "elapsed_minutes"  => elapsed,
        "flight_minutes"   => Int(flight_mins),
        "layover_minutes"  => layover,
        "distance_miles"   => round(Float64(itn.total_distance); digits=0),
        "circuity"         => round(Float64(itn.circuity); digits=2),
        # Status flags (display as independent badges)
        "is_international" => is_international(itn.status),
        "is_codeshare"     => is_codeshare_itn,
        "is_interline"     => is_interline_itn,
        "has_through"      => is_through(itn.status),
        "legs"             => leg_dicts,
        "cnxs"             => cnx_dicts,
    )
end

function _itinref_entry(itn::ItineraryRef, idx::Int; date::String="")
    Dict{String,Any}(
        "date"            => date,
        "idx"             => idx,
        "flights"         => flights_str(itn),
        "route"           => route_str(itn),
        "origin"          => String(origin(itn)),
        "destination"     => String(destination(itn)),
        "num_stops"       => itn.num_stops,
        "elapsed_minutes" => Int(itn.elapsed_minutes),
        "flight_minutes"  => Int(itn.flight_minutes),
        "layover_minutes" => Int(itn.layover_minutes),
        "distance_miles"  => round(Float64(itn.distance_miles); digits=0),
        "circuity"        => round(Float64(itn.circuity); digits=2),
        "legs"            => [Dict{String,Any}(
            "carrier"                             => strip(String(k.carrier)),
            "flight_number"                       => Int(k.flight_number),
            "departure_station"                   => strip(String(k.departure_station)),
            "arrival_station"                     => strip(String(k.arrival_station)),
            "operating_carrier"                   => strip(String(k.operating_carrier)),
            "operating_flight_number"             => Int(k.operating_flight_number),
            "row_number" => Int(k.row_number),
            "record_serial" => Int(k.record_serial),
        ) for k in itn.legs],
    )
end

function _itinrefs_to_json(itins::Vector{ItineraryRef})::String
    JSON3.write([_itinref_entry(itn, i) for (i, itn) in enumerate(itins)])
end

function _write_itinref_html(path::String, json_data::String, title::String)
    page_title = isempty(title) ? "Itinerary Reference Table" : title
    esc_title = _viz_escape_title(page_title)

    html = string(
    "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n",
    "  <meta charset=\"UTF-8\">\n",
    "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n",
    "  <title>", esc_title, "</title>\n",
    "  <style>\n",
    "    * { box-sizing: border-box; margin: 0; padding: 0; }\n",
    "    body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', monospace; font-size: 13px; }\n",
    "    #header { background: #161b22; padding: 12px 20px; border-bottom: 1px solid #30363d; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }\n",
    "    #header h1 { font-size: 16px; font-weight: 600; white-space: nowrap; }\n",
    "    .filter-group { display: flex; gap: 8px; align-items: center; }\n",
    "    .filter-group label { font-size: 11px; color: #8b949e; text-transform: uppercase; }\n",
    "    .filter-group input, .filter-group select { background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; padding: 4px 8px; border-radius: 4px; font-size: 12px; }\n",
    "    #stats { margin-left: auto; font-size: 11px; color: #8b949e; }\n",
    "    #container { overflow-x: auto; padding: 8px; }\n",
    "    table { width: 100%; border-collapse: collapse; }\n",
    "    th { background: #161b22; color: #8b949e; font-size: 11px; text-transform: uppercase; padding: 8px 10px; text-align: left; cursor: pointer; user-select: none; border-bottom: 1px solid #30363d; position: sticky; top: 0; white-space: nowrap; }\n",
    "    th:hover { color: #c9d1d9; }\n",
    "    th .arrow { font-size: 10px; margin-left: 4px; }\n",
    "    td { padding: 6px 10px; border-bottom: 1px solid #21262d; white-space: nowrap; }\n",
    "    tr:hover { background: #161b22; }\n",
    "    tr.expanded { background: #1c2333; }\n",
    "    .stops-0 td:first-child { border-left: 3px solid #4ef17a; }\n",
    "    .stops-1 td:first-child { border-left: 3px solid #f1e14e; }\n",
    "    .stops-2 td:first-child { border-left: 3px solid #f1a14e; }\n",
    "    .stops-3 td:first-child { border-left: 3px solid #f14e4e; }\n",
    "    .leg-detail { background: #0d1117; }\n",
    "    .leg-detail td { padding: 4px 10px 4px 30px; color: #8b949e; font-size: 12px; border-bottom: 1px solid #161b22; }\n",
    "    .leg-detail td:first-child { border-left: 3px solid #30363d; }\n",
    "    .cnx-detail { background: #0a0e15; }\n",
    "    .cnx-detail td { padding: 4px 10px 4px 50px; color: #6e7681; font-size: 11px; font-style: italic; border-bottom: 1px solid #161b22; }\n",
    "    .cnx-detail td:first-child { border-left: 3px solid #30363d; }\n",
    "    .route { color: #58a6ff; } .flights { color: #d2a8ff; } .time { color: #79c0ff; } .dist { color: #7ee787; } .circ { color: #ffa657; }\n",
    "    .badge { display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 10px; font-weight: 600; margin-right: 3px; }\n",
    "    .badge-intl { background: #1f3d5c; color: #79c0ff; }\n",
    "    .badge-cs   { background: #3d2c5c; color: #d2a8ff; }\n",
    "    .badge-il   { background: #5c3d1f; color: #ffa657; }\n",
    "    .badge-thr  { background: #2c5c3d; color: #7ee787; }\n",
    "    .trc-badge  { background: #5c1f2c; color: #ff7b8b; padding: 0 4px; border-radius: 2px; font-size: 10px; margin-left: 4px; }\n",
    "    .mct-src-exception { color: #ff7b8b; }\n",
    "    .mct-src-standard  { color: #ffa657; }\n",
    "    .mct-src-default   { color: #6e7681; }\n",
    "    .ts { color: #79c0ff; font-family: monospace; }\n",
    "    .clickable { cursor: pointer; }\n",
    "    .expand-icon { display: inline-block; width: 16px; text-align: center; color: #484f58; }\n",
    "  </style>\n</head>\n<body>\n",
    "  <div id=\"header\">\n",
    "    <h1>", page_title, "</h1>\n",
    "    <div class=\"filter-group\">\n",
    "      <label>Origin</label><input id=\"f-org\" placeholder=\"e.g. ORD\" size=\"5\">\n",
    "      <label>Dest</label><input id=\"f-dst\" placeholder=\"e.g. LHR\" size=\"5\">\n",
    "      <label>Stops</label><select id=\"f-stops\"><option value=\"\">All</option><option value=\"0\">Nonstop</option><option value=\"1\">1-stop</option><option value=\"2\">2-stop</option></select>\n",
    "    </div>\n",
    "    <div id=\"stats\"></div>\n",
    "  </div>\n",
    "  <div id=\"container\">\n",
    "    <table><thead><tr>\n",
    "      <th data-col=\"idx\">#<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"origin\">Origin<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"destination\">Dest<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"flights\">Flights<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"route\">Route<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"num_stops\">Stops<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"elapsed_minutes\">Elapsed<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"flight_minutes\">Flight<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"layover_minutes\">Layover<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"distance_miles\">Distance<span class=\"arrow\"></span></th>\n",
    "      <th data-col=\"circuity\">Circuity<span class=\"arrow\"></span></th>\n",
    "      <th>Flags</th>\n",
    "    </tr></thead><tbody id=\"tbody\"></tbody></table>\n",
    "  </div>\n",
    "  <script>\n",
    "    const DATA = ", json_data, ";\n",
    "    let sortCol = 'num_stops', sortAsc = true;\n",
    "    let expanded = new Set();\n",
    "    function fmtTime(m) { const h = Math.floor(m/60), mm = m%60; return h+'h'+(mm<10?'0':'')+mm; }\n",
    "    function fmtDt(s) {\n",
    "      // Compact ISO display: \"2026-03-18T22:30:00Z\" -> \"03-18 22:30\"\n",
    "      if (!s) return '';\n",
    "      const m = s.match(/^(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2})/);\n",
    "      return m ? (m[2]+'-'+m[3]+' '+m[4]+':'+m[5]+'Z') : s;\n",
    "    }\n",
    "    function addCell(tr, text, cls) {\n",
    "      const td = document.createElement('td');\n",
    "      td.textContent = text;\n",
    "      if (cls) td.className = cls;\n",
    "      tr.appendChild(td);\n",
    "      return td;\n",
    "    }\n",
    "    function appendDetailRow(cls, tbody) {\n",
    "      const r = document.createElement('tr');\n",
    "      r.className = cls;\n",
    "      tbody.appendChild(r);\n",
    "      return r;\n",
    "    }\n",
    "    function appendBadgeText(parent, text, color, cls) {\n",
    "      const span = document.createElement('span');\n",
    "      span.textContent = ' ' + text;\n",
    "      if (cls) span.className = cls;\n",
    "      else span.style.color = color || '#8b949e';\n",
    "      parent.appendChild(span);\n",
    "    }\n",
    "    function render() {\n",
    "      const fOrg = document.getElementById('f-org').value.toUpperCase().trim();\n",
    "      const fDst = document.getElementById('f-dst').value.toUpperCase().trim();\n",
    "      const fStops = document.getElementById('f-stops').value;\n",
    "      let filtered = DATA.filter(d => {\n",
    "        if (fOrg && d.origin !== fOrg) return false;\n",
    "        if (fDst && d.destination !== fDst) return false;\n",
    "        if (fStops !== '' && d.num_stops !== parseInt(fStops)) return false;\n",
    "        return true;\n",
    "      });\n",
    "      filtered.sort((a,b) => {\n",
    "        let va=a[sortCol], vb=b[sortCol];\n",
    "        if (typeof va==='string') { va=va.toLowerCase(); vb=vb.toLowerCase(); }\n",
    "        if (va<vb) return sortAsc?-1:1; if (va>vb) return sortAsc?1:-1; return 0;\n",
    "      });\n",
    "      const tbody = document.getElementById('tbody');\n",
    "      tbody.innerHTML = '';\n",
    "      filtered.forEach((d,fi) => {\n",
    "        const key = d.origin+d.destination+d.idx+(d.date||'');\n",
    "        const tr = document.createElement('tr');\n",
    "        tr.className = 'clickable stops-'+Math.min(d.num_stops,3)+(expanded.has(key)?' expanded':'');\n",
    "        const flagsHtml = (d.is_international?'<span class=\"badge badge-intl\">INTL</span>':'')+\n",
    "                          (d.is_codeshare?'<span class=\"badge badge-cs\">CS</span>':'')+\n",
    "                          (d.is_interline?'<span class=\"badge badge-il\">IL</span>':'')+\n",
    "                          (d.has_through?'<span class=\"badge badge-thr\">THR</span>':'');\n",
    "        tr.innerHTML = '<td><span class=\"expand-icon\">'+(expanded.has(key)?'&#9660;':'&#9654;')+'</span> '+(fi+1)+'</td>'+\n",
    "          '<td>'+d.origin+'</td><td>'+d.destination+'</td>'+\n",
    "          '<td class=\"flights\">'+d.flights+'</td><td class=\"route\">'+d.route+'</td>'+\n",
    "          '<td>'+d.num_stops+'</td>'+\n",
    "          '<td class=\"time\">'+fmtTime(d.elapsed_minutes)+'</td>'+\n",
    "          '<td class=\"time\">'+fmtTime(d.flight_minutes)+'</td>'+\n",
    "          '<td class=\"time\">'+fmtTime(d.layover_minutes)+'</td>'+\n",
    "          '<td class=\"dist\">'+d.distance_miles.toLocaleString()+' mi</td>'+\n",
    "          '<td class=\"circ\">'+d.circuity.toFixed(2)+'</td>'+\n",
    "          '<td>'+flagsHtml+'</td>';\n",
    "        tr.onclick = () => { expanded.has(key)?expanded.delete(key):expanded.add(key); render(); };\n",
    "        tbody.appendChild(tr);\n",
    "        if (expanded.has(key) && d.legs) {\n",
    "          d.legs.forEach((leg, li) => {\n",
    "            const lr = appendDetailRow('leg-detail', tbody);\n",
    "            // is_codeshare_leg is precomputed server-side using the SSIM\n",
    "            // convention (empty operating_carrier or operating_flight_number=0\n",
    "            // means host flight, where marketing IS operating).\n",
    "            const isCS = !!leg.is_codeshare_leg;\n",
    "            addCell(lr, 'L'+(li+1));\n",
    "            addCell(lr, leg.departure_station);\n",
    "            addCell(lr, leg.arrival_station);\n",
    "            const fltCell = addCell(lr, leg.carrier+' '+leg.flight_number);\n",
    "            if (isCS) appendBadgeText(fltCell, '(op '+leg.operating_carrier+' '+leg.operating_flight_number+')', '#8b949e');\n",
    "            if (leg.dei_10) appendBadgeText(fltCell, 'also: '+leg.dei_10, '#79c0ff');\n",
    "            if (leg.trc) appendBadgeText(fltCell, 'TRC '+leg.trc, '#ff7b8b', 'trc-badge');\n",
    "            if (leg.dep_dt) {\n",
    "              const tsCell = addCell(lr, fmtDt(leg.dep_dt)+' \\u2192 '+fmtDt(leg.arr_dt), 'ts');\n",
    "              tsCell.colSpan = 2;\n",
    "            } else {\n",
    "              addCell(lr, '').colSpan = 2;\n",
    "            }\n",
    "            const meta = addCell(lr, 'row='+leg.row_number+' serial='+leg.record_serial+(leg.aircraft_type?' \\u00b7 '+leg.aircraft_type:''));\n",
    "            meta.colSpan = 3;\n",
    "            meta.style.color = '#6e7681'; meta.style.fontSize = '11px';\n",
    "            addCell(lr, '').colSpan = 2;\n",
    "            // Connection row after this leg, if a cnx follows it (rich format only)\n",
    "            if (d.cnxs && li < d.cnxs.length) {\n",
    "              const c = d.cnxs[li];\n",
    "              const cr = appendDetailRow('cnx-detail', tbody);\n",
    "              addCell(cr, '\\u21b3 cnx');\n",
    "              const stCell = addCell(cr, 'at '+c.station); stCell.colSpan = 3;\n",
    "              if (c.is_through) appendBadgeText(stCell, 'THR', '#7ee787', 'badge badge-thr');\n",
    "              const cnxCell = addCell(cr, c.cnx_time+'min cnx (MCT '+c.mct+'min)'); cnxCell.colSpan = 2;\n",
    "              const matchCell = document.createElement('td'); matchCell.colSpan = 3;\n",
    "              matchCell.appendChild(document.createTextNode('matched: '));\n",
    "              const srcSpan = document.createElement('span'); srcSpan.className = 'mct-src-'+c.mct_source; srcSpan.textContent = c.mct_source;\n",
    "              matchCell.appendChild(srcSpan);\n",
    "              matchCell.appendChild(document.createTextNode(' mct_id='+c.mct_id+(c.mct_time!==c.mct?' time='+c.mct_time:'')));\n",
    "              cr.appendChild(matchCell);\n",
    "              addCell(cr, '').colSpan = 2;\n",
    "            }\n",
    "          });\n",
    "        }\n",
    "      });\n",
    "      document.getElementById('stats').textContent = filtered.length+' of '+DATA.length+' itineraries';\n",
    "    }\n",
    "    document.querySelectorAll('th[data-col]').forEach(th => {\n",
    "      th.onclick = () => {\n",
    "        const col=th.dataset.col;\n",
    "        if (sortCol===col) sortAsc=!sortAsc; else { sortCol=col; sortAsc=true; }\n",
    "        document.querySelectorAll('th .arrow').forEach(a => a.textContent='');\n",
    "        th.querySelector('.arrow').textContent = sortAsc?' \\u25B2':' \\u25BC';\n",
    "        render();\n",
    "      };\n",
    "    });\n",
    "    document.querySelectorAll('.filter-group input, .filter-group select').forEach(el => { el.oninput=render; });\n",
    "    render();\n",
    "  </script>\n",
    "</body>\n</html>")

    mkpath(dirname(abspath(path)))
    write(path, html)
    return nothing
end
