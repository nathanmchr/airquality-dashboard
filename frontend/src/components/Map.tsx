import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";

function Map() {
  const position = [51.505, -0.09];
  return (
    <div className="bg-white flex flex-col h-full md:w-[70%] xl:w-[75%] 2xl:w-[80%] p-4 rounded-xl shadow-xl">
      <h1>Map</h1>
      <div className="flex-1">
        <MapContainer
          center={[51.505, -0.09]}
          zoom={13}
          scrollWheelZoom={false}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <Marker position={[51.505, -0.09]}>
            <Popup>
              A pretty CSS3 popup. <br /> Easily customizable.
            </Popup>
          </Marker>
        </MapContainer>
      </div>
    </div>
  );
}

export default Map;
