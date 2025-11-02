import { MapContainer, TileLayer, Tooltip, CircleMarker } from "react-leaflet";

type Measurement = {
  id: number;
  start_time: string;
  end_time: string;
  organization: string;
  site_name: string;
  pollutant: string;
  raw_value: number;
  unit: string;
  quality_code: string;
  validity: number;
  city: string;
  longitude: number;
  latitude: number;
};

type MapProps = {
  data: Measurement[] | null;
  onSelectSite: (siteName: string) => void;
  selectedSite: string | null;
};

function getColor(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return "#999999"; // grey if no value
  if (value < 10) return "#4CAF50"; // green
  if (value < 20) return "#FFC107"; // yellow
  if (value < 35) return "#FF9800"; // orange
  if (value < 50) return "#F44336"; // red
  return "#9C27B0"; // purple
}

function lightenColor(color: string, percent: number): string {
  // transforme "#rrggbb" en RGB
  const num = parseInt(color.slice(1), 16);
  let r = (num >> 16) + percent;
  let g = ((num >> 8) & 0x00ff) + percent;
  let b = (num & 0x0000ff) + percent;

  r = Math.min(255, Math.max(0, r));
  g = Math.min(255, Math.max(0, g));
  b = Math.min(255, Math.max(0, b));

  return `rgb(${r},${g},${b})`;
}

function Map({ data, onSelectSite, selectedSite }: MapProps) {
  return (
    <div className="bg-white flex flex-col h-auto lg:h-full w-full lg:w-[70%] p-4 rounded-xl shadow-xl gap-2">
      <h1>Map</h1>
      <div className="flex-1 min-h-[60vh]">
        <MapContainer center={[46.9, 1.8]} zoom={6} scrollWheelZoom={true}>
          <TileLayer
            attribution='Data by &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>, under <a href="https://opendatacommons.org/licenses/odbl/">ODbL.</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          {/* Markers when there is data */}
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
                      ? "rgba(255,255,255,0.7)"
                      : "transparent",
                  weight: m.site_name === selectedSite ? 2 : 0,
                  fillColor:
                    m.site_name === selectedSite
                      ? lightenColor(color, 60)
                      : color,
                  fillOpacity: 0.8,
                }}
                eventHandlers={{ click: () => onSelectSite(m.site_name) }}
              >
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
        </MapContainer>
      </div>
    </div>
  );
}

export default Map;
