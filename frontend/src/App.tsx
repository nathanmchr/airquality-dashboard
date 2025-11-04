import Info from "./components/Info";
import Map from "./components/Map";
import { useState, useEffect } from "react";
import { getData } from "./apiCalls";

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
  raw_value: number; // Measured numeric value
  unit: string; // Unit of measurement (e.g., µg/m³)
  quality_code: string; // Data quality code
  validity: number; // Validity flag or percentage
  city: string; // City where the measurement was taken
  longitude: number; // Geographic longitude of the site
  latitude: number; // Geographic latitude of the site
};

/**
 * Main App component.
 * Handles data fetching, state management, and layout.
 */
function App() {
  // State for storing all latest site measurements
  const [sites, setSites] = useState<Measurement[] | null>(null);

  // State for tracking the currently selected site
  const [selectedSite, setSelectedSite] = useState<string | null>(null);

  /**
   * Fetch the latest measurements from the API when the component mounts.
   */
  useEffect(() => {
    async function fetchData() {
      try {
        const data = await getData<Measurement[]>(
          "http://127.0.0.1:8000/measurements/latest"
        );

        if (data) {
          // Optionally, filter out old measurements here if needed
          setSites(data);
        }
      } catch (error) {
        console.error("Error fetching measurement data:", error);
      }
    }

    fetchData();
  }, []);

  return (
    <div className="flex flex-col h-screen">
      {/* Header section */}
      <header className="flex items-center gap-2 p-4 bg-green-700 text-white font-bold">
        <img src="/icon.svg" alt="App Icon" className="w-8 h-8" />
        France Air Quality
      </header>

      {/* Main page layout: Map + Info panel */}
      <div className="page flex flex-col lg:flex-row">
        {/* Map container */}
        <div className="transition-all duration-400 flex-1 lg:w-[70%] w-full">
          <Map
            data={sites} // Pass measurement data to the Map component
            onSelectSite={setSelectedSite} // Callback when a site is selected
            selectedSite={selectedSite} // Currently selected site
          />
        </div>

        {/* Info panel */}
        <div className="transition-all duration-400 lg:w-[30%] w-full">
          <Info selectedSite={selectedSite} />{" "}
          {/* Display info for the selected site */}
        </div>
      </div>
    </div>
  );
}

export default App;
