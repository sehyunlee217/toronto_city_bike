// Both maps use the same zoom thresholds. Higher H3 resolution means smaller cells.
function resolutionForZoom(zoom) {
  if (zoom <= 9) return 5;
  if (zoom <= 10) return 6;
  if (zoom <= 11) return 7;
  if (zoom <= 13) return 8;
  if (zoom <= 15) return 9;
  if (zoom <= 17) return 10;
  return 11;
}
