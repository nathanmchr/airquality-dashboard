import Info from "./components/Info";
import Map from "./components/Map";

function App() {
  return (
    <>
      <header>France Air Quality</header>
      <div className="flex flex-row">
        <Info></Info>
        <Map></Map>
      </div>
    </>
  );
}

export default App;
