import Info from "./components/Info";
import Map from "./components/Map";

function App() {
  return (
    <div className="flex flex-col h-screen">
      <header>France Air Quality</header>
      <div className="flex flex-1 flex-col md:flex-row p-4 space-y-4 md:space-x-4 bg-gray-100">
        <Map></Map>
        <Info></Info>
      </div>
    </div>
  );
}

export default App;
