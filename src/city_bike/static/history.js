const $ = (id) => document.getElementById(id);
const map = L.map("map").setView([43.665, -79.39], 12);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);
const colors = {
  empty: "#db3950",
  low: "#f1a52c",
  healthy: "#158b78",
  full: "#3869d4",
  unknown: "#89939f",
};
const clock = (value) =>
  new Date(value).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
let data = null,
  layer = null,
  playing = null,
  selected = null,
  loading = false,
  following = true;
function pause() {
  clearInterval(playing);
  playing = null;
  $("play").textContent = "Play";
}
function details(cell) {
  $("details").replaceChildren();
  const title = document.createElement("h2");
  title.textContent = cell ? cell.h3_cell : "Station details";
  const body = document.createElement("p");
  body.textContent = cell
    ? `${cell.station_count} stations · ${cell.total_bikes_available ?? "Unknown"} bikes · ${cell.total_docks_available ?? "Unknown"} docks · ${cell.availability_ratio === null ? "Unknown availability" : Math.round(cell.availability_ratio * 100) + "% available"}`
    : "Select a hexagon on the map.";
  $("details").append(title, body);
}
let frameRequest = 0,
  frameController = null,
  dragTimer = null,
  rendering = false;
// Cancel older requests so fast dragging cannot display the wrong timestamp.
async function render() {
  if (!data?.frames.length) return;
  const moment = data.frames[Number($("timeline").value)];
  const at = moment.at;
  const requestId = ++frameRequest;
  if (frameController) frameController.abort();
  frameController = new AbortController();
  rendering = true;
  $("selected-time").textContent = clock(at);
  $("status").textContent = "Loading selected recording…";
  try {
    const response = await fetch(
      `/history/data?resolution=${resolutionForZoom(map.getZoom())}&at=${moment.timestamp}&generation=${data.generation}`,
      { signal: frameController.signal },
    );
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Could not load the selected recording.");
    }
    const snapshot = await response.json();
    if (requestId !== frameRequest) return;
    const frame = snapshot.frames[0];
    if (!frame) throw new Error(snapshot.error || "No history available.");
    showFrame(frame, snapshot.geometries);
  } catch (error) {
    if (error.name !== "AbortError") {
      $("status").textContent = error.message;
      $("status").className = "error";
      pause();
    }
  } finally {
    if (requestId === frameRequest) rendering = false;
  }
}
// Loading a range reads the archive; dragging only asks for individual frames.
async function refresh() {
  if (loading || playing) return;
  loading = true;
  $("load-range").disabled = true;
  $("status").textContent = "Reading archive files from S3…";
  try {
    const start = new Date($("range-start").value).getTime() / 1000;
    const end = new Date($("range-end").value).getTime() / 1000;
    if (
      !Number.isFinite(start) ||
      !Number.isFinite(end) ||
      end <= start ||
      end - start > 86400
    )
      throw new Error("Choose a valid range up to 24 hours.");
    const response = await fetch(`/history/timeline?start=${start}&end=${end}`);
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "History request failed.");
    }
    const next = await response.json();
    $("archive-range").textContent = next.available_start
      ? `Archive spans ${clock(next.available_start)} – ${clock(next.available_end)}.`
      : "No archived observations found.";
    if (playing) return;
    const oldTime = data?.frames[Number($("timeline").value)]?.at;
    if (!next.frames.length) {
      pause();
      data = next;
      if (layer) {
        map.removeLayer(layer);
        layer = null;
      }
      for (const id of ["play", "timeline", "latest"]) $(id).disabled = true;
      $("status").textContent =
        next.error ||
        next.warning ||
        "No archived readings in this range. Select a time within the archive span.";
      $("status").className = "error";
      return;
    }
    data = next;
    $("timeline").max = data.frames.length - 1;
    const index =
      following || !oldTime
        ? data.frames.length - 1
        : data.frames.findIndex((f) => f.at >= oldTime);
    $("timeline").value = index < 0 ? data.frames.length - 1 : index;
    for (const id of ["play", "timeline", "latest"]) $(id).disabled = false;
    render();
  } catch (error) {
    $("status").textContent =
      `${error.message} Showing the last loaded history, if available.`;
    $("status").className = "error";
  } finally {
    loading = false;
    $("load-range").disabled = false;
  }
}
$("timeline").addEventListener("input", () => {
  pause();
  following = false;
  clearTimeout(dragTimer);
  $("selected-time").textContent = clock(
    data.frames[Number($("timeline").value)].at,
  );
  dragTimer = setTimeout(render, 80);
});
$("play").addEventListener("click", () => {
  if (playing) {
    pause();
    return;
  }
  following = false;
  if (Number($("timeline").value) >= data.frames.length - 1)
    $("timeline").value = 0;
  render();
  $("play").textContent = "Pause";
  playing = setInterval(() => {
    if (rendering) return;
    const index = Number($("timeline").value);
    if (index >= data.frames.length - 1) {
      pause();
      return;
    }
    $("timeline").value = index + 1;
    render();
  }, 500);
});
$("latest").addEventListener("click", () => {
  pause();
  following = true;
  $("timeline").value = data.frames.length - 1;
  render();
});
map.on("zoomend", () => {
  if (!loading) render();
});
function localInput(date) {
  return new Date(date.getTime() - date.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
}
$("range-end").value = localInput(new Date());
$("range-start").value = localInput(new Date(Date.now() - 3600000));
$("range-form").addEventListener("submit", (event) => {
  event.preventDefault();
  pause();
  following = true;
  ++frameRequest;
  if (frameController) frameController.abort();
  for (const id of ["play", "timeline", "latest"]) $(id).disabled = true;
  refresh();
});

// Update the map and sidebar only after the selected frame has arrived.
function showFrame(frame, geometries) {
  $("selected-time").textContent = clock(frame.at);
  $("timeline").setAttribute("aria-valuetext", clock(frame.at));
  $("start-time").textContent = clock(data.frames[0].at);
  $("end-time").textContent = clock(data.frames.at(-1).at);
  $("stations").textContent = frame.station_count.toLocaleString();
  $("cells").textContent = frame.cells.length.toLocaleString();
  const knownCells = frame.cells.filter(
    (c) => c.total_bikes_available !== null,
  );
  $("bikes").textContent = knownCells.length
    ? knownCells
        .reduce((sum, c) => sum + c.total_bikes_available, 0)
        .toLocaleString()
    : "—";
  const features = frame.cells.map((c) => ({
    type: "Feature",
    geometry: geometries[c.h3_cell],
    properties: c,
  }));
  if (layer) map.removeLayer(layer);
  layer = L.geoJSON(
    { type: "FeatureCollection", features },
    {
      style: (f) => ({
        color: colors[f.properties.supply_level],
        weight: 1.5,
        fillOpacity: 0.58,
      }),
      onEachFeature: (f, l) => {
        l.bindTooltip(
          `${f.properties.station_count} stations · ${f.properties.total_bikes_available ?? "Unknown"} bikes`,
        );
        l.on("click", () => {
          selected = f.properties.h3_cell;
          details(f.properties);
        });
      },
    },
  ).addTo(map);
  details(frame.cells.find((c) => c.h3_cell === selected));
  $("status").className = data.status === "error" ? "error" : "";
  $("status").textContent =
    data.error ||
    data.warning ||
    (frame.cells.length
      ? `Recorded at ${clock(frame.at)}. ${frame.stale_stations} stale stations; ${frame.missing_metadata} missing metadata.`
      : "No matching recorded stations at this time. Keep collection running to build history.");
}
