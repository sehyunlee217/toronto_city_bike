"use strict";
const statusEl = document.getElementById("status");
const details = document.getElementById("details");
const colors = {
  empty: "#db3950",
  low: "#f1a52c",
  healthy: "#158b78",
  full: "#3869d4",
  unknown: "#525961",
};
const number = (value) => Number(value).toLocaleString();

if (!window.L) {
  statusEl.textContent =
    "The map library could not load. Check your internet connection and reload the page.";
  statusEl.className = "error";
} else {
  const map = L.map("map").setView([43.68, -79.39], 12);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);
  let layer;
  let selectedId;
  let controller;
  let requestId = 0;
  let moveTimer;
  let lastFetched;

  function showDetails(p) {
    details.replaceChildren();
    const heading = document.createElement("h2");
    heading.textContent = "Selected hexagon";
    const id = document.createElement("code");
    id.textContent = p.h3_cell;
    if (p.station_count === 0) {
      const message = document.createElement("p");
      message.textContent =
        "No stations reported in this cell. This is not an empty bike station.";
      details.append(heading, id, message);
      return;
    }
    const list = document.createElement("dl");
    const rows = {
      Stations: number(p.station_count),
      "Available bikes": number(p.total_bikes_available),
      "Available docks": number(p.total_docks_available),
      Capacity: number(p.total_capacity),
      "Bike availability":
        p.availability_ratio === null
          ? "Unknown"
          : `${Math.round(p.availability_ratio * 100)}%`,
      "Empty stations": number(p.empty_station_count),
      "Low supply stations": number(p.low_station_count),
    };
    for (const [label, value] of Object.entries(rows)) {
      const term = document.createElement("dt");
      term.textContent = label;
      const definition = document.createElement("dd");
      definition.textContent = value;
      list.append(term, definition);
    }
    details.append(heading, id, list);
  }

  function selectCell(feature, polygon) {
    selectedId = feature.properties.h3_cell;
    layer.resetStyle();
    polygon.setStyle({ color: "#132e3e", weight: 3, fillOpacity: 0.8 });
    polygon.bringToFront();
    showDetails(feature.properties);
  }

  async function refresh() {
    const currentRequest = ++requestId;
    if (controller) controller.abort();
    controller = new AbortController();
    const timeout = setTimeout(() => controllerForRequest.abort(), 30000);
    const controllerForRequest = controller;
    statusEl.textContent = "Fetching current availability…";
    statusEl.className = "";
    try {
      const requestedResolution = resolutionForZoom(map.getZoom());
      const bounds = map.getBounds();
      const params = new URLSearchParams({
        resolution: requestedResolution,
        west: Math.max(-180, bounds.getWest()),
        east: Math.min(180, bounds.getEast()),
        south: Math.max(-85, bounds.getSouth()),
        north: Math.min(85, bounds.getNorth()),
      });
      const response = await fetch(`/h3/map-data?${params}`, {
        signal: controllerForRequest.signal,
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (currentRequest !== requestId) return;
      const next = L.geoJSON(data, {
        style: (feature) =>
          feature.properties.station_count === 0
            ? {
                color: "#768596",
                weight: 0.7,
                opacity: 0.35,
                fillColor: "#89939f",
                fillOpacity: 0.1,
              }
            : {
                color: "#fff",
                weight: 1.3,
                fillColor:
                  colors[feature.properties.supply_level] || colors.unknown,
                fillOpacity: 0.64,
              },
        onEachFeature: (feature, polygon) => {
          const p = feature.properties;
          const tip = document.createElement("span");
          tip.textContent = p.station_count
            ? `${number(p.total_bikes_available)} bikes · ${number(p.station_count)} stations`
            : "No stations in this cell";
          polygon.bindTooltip(tip);
          polygon.on("click", () => selectCell(feature, polygon));
          polygon.on("add", () => {
            const path = polygon.getElement();
            if (!path) return;
            path.setAttribute("tabindex", "0");
            path.setAttribute("role", "button");
            path.setAttribute(
              "aria-label",
              `Cell ${p.h3_cell}: ${tip.textContent}`,
            );
            path.addEventListener("keydown", (event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                selectCell(feature, polygon);
              }
            });
          });
        },
      });
      if (layer) map.removeLayer(layer);
      layer = next.addTo(map);
      layer.eachLayer((polygon) => {
        if (polygon.feature.properties.station_count) polygon.bringToFront();
      });
      document.getElementById("scale").textContent =
        "Hexagons combine as you zoom out. Zoom in for station detail.";
      let selectedFound = false;
      layer.eachLayer((polygon) => {
        if (polygon.feature.properties.h3_cell === selectedId) {
          selectedFound = true;
          selectCell(polygon.feature, polygon);
        }
      });
      if (!selectedFound) {
        selectedId = null;
        details.innerHTML =
          "<h2>Station details</h2><p>Select a hexagon on the map.</p>";
      }
      const totals = data.features.reduce(
        (sum, f) => ({
          bikes: sum.bikes + f.properties.total_bikes_available,
          stations: sum.stations + f.properties.station_count,
        }),
        { bikes: 0, stations: 0 },
      );
      document.getElementById("bikes").textContent = number(totals.bikes);
      document.getElementById("stations").textContent = number(totals.stations);
      document.getElementById("cells").textContent = number(
        data.features.filter((f) => f.properties.station_count > 0).length,
      );
      lastFetched = new Date(data.fetched_at).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
      statusEl.textContent = totals.stations
        ? `Fetched at ${lastFetched} · refreshes every 60s`
        : "No station data available. Retrying in 60 seconds.";
    } catch (error) {
      if (currentRequest !== requestId) return;
      statusEl.className = "error";
      statusEl.textContent = lastFetched
        ? `Refresh failed. Showing data fetched at ${lastFetched}. Retrying in 60s.`
        : "Could not fetch bike availability. Check your connection. Retrying in 60 seconds.";
    } finally {
      clearTimeout(timeout);
    }
  }
  map.on("moveend", () => {
    clearTimeout(moveTimer);
    moveTimer = setTimeout(refresh, 180);
  });
  refresh();
  setInterval(() => {
    if (!document.hidden) refresh();
  }, 60000);
}
