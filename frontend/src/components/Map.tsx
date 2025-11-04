import { useEffect } from "react";
import {
  MapContainer,
  TileLayer,
  Tooltip,
  CircleMarker,
  useMap,
} from "react-leaflet";
import L, { Control } from "leaflet";

/**
 * Type definition for a single air quality measurement.
 */
type Measurement = {
  id: number; // Unique identifier for the measurement
  start_time: string; // ISO string for measurement start time
  end_time: string; // ISO string for measurement end time
  organization: string; // Organization providing the measurement
  site_name: string; // Name of the monitoring site
  pollutant: string; // Pollutant being measured (e.g., PM2.5)
  raw_value: number; // Numeric value of the measurement
  unit: string; // Unit of measurement (e.g., µg/m³)
  quality_code: string; // Data quality code
  validity: number; // Validity flag or percentage
  city: string; // City where measurement was taken
  longitude: number; // Geographic longitude
  latitude: number; // Geographic latitude
};

/**
 * Props for the Map component
 */
type MapProps = {
  data: Measurement[] | null; // Array of measurements to display
  onSelectSite: (siteName: string) => void; // Callback when a site is clicked
  selectedSite: string | null; // Currently selected site
};

/**
 * Returns a color based on the measurement value
 * @param value Numeric value of the measurement
 */
function getColor(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return "#999999"; // Grey if no value
  if (value < 10) return "#4CAF50"; // Green
  if (value < 20) return "#FFC107"; // Yellow
  if (value < 35) return "#FF9800"; // Orange
  if (value < 50) return "#F44336"; // Red
  return "#9C27B0"; // Purple for high values
}

/**
 * Lightens a given hex color by a percentage
 * @param color Hex color string (e.g., "#FF0000")
 * @param percent Number to increase RGB channels by
 */
function lightenColor(color: string, percent: number): string {
  const num = parseInt(color.slice(1), 16);
  let r = (num >> 16) + percent;
  let g = ((num >> 8) & 0x00ff) + percent;
  let b = (num & 0x0000ff) + percent;

  // Clamp values between 0 and 255
  r = Math.min(255, Math.max(0, r));
  g = Math.min(255, Math.max(0, g));
  b = Math.min(255, Math.max(0, b));

  return `rgb(${r},${g},${b})`;
}

/**
 * Custom legend control for the map
 */
function Legend() {
  const map = useMap(); // Access the Leaflet map instance

  useEffect(() => {
    // Create a Leaflet control positioned in the bottom-left
    const legend = new Control({ position: "bottomleft" });

    // Define categories and their colors
    const categories = [
      { color: "#999999", label: "Unavailable" },
      { color: "#4CAF50", label: "0-10 µg-m3" },
      { color: "#FFC107", label: "10-20 µg-m3" },
      { color: "#FF9800", label: "20-35 µg-m3" },
      { color: "#F44336", label: "35-50 µg-m3" },
      { color: "#9C27B0", label: "50+ µg-m3" },
    ];

    // Render the legend HTML
    legend.onAdd = () => {
      const div = L.DomUtil.create("div", "map-legend");

      categories.forEach((cat) => {
        const row = document.createElement("div");
        row.style.display = "flex";
        row.style.alignItems = "center";
        row.style.gap = "4px";

        const colorBox = document.createElement("span");
        colorBox.style.display = "inline-block";
        colorBox.style.width = "12px";
        colorBox.style.height = "12px";
        colorBox.style.backgroundColor = cat.color;
        colorBox.style.borderRadius = "3px";

        const label = document.createElement("span");
        label.innerText = cat.label;

        row.appendChild(colorBox);
        row.appendChild(label);
        div.appendChild(row);
      });

      return div;
    };

    legend.addTo(map); // Add legend to the map

    // Cleanup on unmount
    return () => {
      legend.remove();
    };
  }, [map]);

  return null; // No JSX needed
}

/**
 * Map component displaying measurements as CircleMarkers with tooltips
 */
function Map({ data, onSelectSite, selectedSite }: MapProps) {
  return (
    <div className="frosted-glass section w-full flex flex-col h-full">
      <h1>Map</h1>
      <div className="flex-1 min-h-[60vh]">
        <MapContainer center={[46.9, 1.8]} zoom={6} scrollWheelZoom={true}>
          {/* Base map tiles */}
          <TileLayer
            attribution='Data by &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>, under <a href="https://opendatacommons.org/licenses/odbl/">ODbL.</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          {/* Map markers */}
          {data?.map((m) => {
            const color = getColor(m.raw_value);

            return (
              <CircleMarker
                key={m.id}
                center={[m.latitude, m.longitude]}
                radius={6}
                pathOptions={{
                  color:
                    m.site_name === selectedSite
                      ? "rgba(255,255,255,0.7)" // highlight stroke for selected
                      : "transparent",
                  weight: m.site_name === selectedSite ? 2 : 0,
                  fillColor:
                    m.site_name === selectedSite
                      ? lightenColor(color, 60) // brighter if selected
                      : color,
                  fillOpacity: 0.8,
                }}
                eventHandlers={{
                  click: () => onSelectSite(m.site_name), // Select site on click

                  // Hover effects
                  mouseover: (e) => {
                    const layer = e.target;
                    layer.setStyle({
                      fillColor: lightenColor(color, 40), // Slightly lighten
                      radius: 8, // Optional: increase size on hover
                    });
                  },
                  mouseout: (e) => {
                    const layer = e.target;
                    layer.setStyle({
                      fillColor:
                        m.site_name === selectedSite
                          ? lightenColor(color, 60)
                          : color,
                      radius: 6, // Reset radius
                    });
                  },
                }}
              >
                {/* Tooltip for each measurement */}
                <Tooltip
                  direction="top"
                  offset={[0, -5]}
                  opacity={1}
                  className="text-sm"
                >
                  <strong>{m.site_name}</strong>
                  <br />
                  {m.pollutant}: {m.raw_value} {m.unit}
                  <br />
                  Measurement time:{" "}
                  {new Date(m.end_time).toLocaleString("fr-FR")}
                </Tooltip>
              </CircleMarker>
            );
          })}

          {/* Add the legend */}
          <Legend />
        </MapContainer>
      </div>
    </div>
  );
}

export default Map;
