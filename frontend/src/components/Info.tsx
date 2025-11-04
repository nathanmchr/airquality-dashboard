import { useEffect, useRef, useState } from "react";
import { formatInTimeZone } from "date-fns-tz";
import * as d3 from "d3";

/**
 * Type definition for a single measurement
 */
type Measurement = {
  id: number; // Unique ID for the measurement
  pollutant: string; // Name of the pollutant
  raw_value: number | null; // Measured value (nullable)
  unit: string; // Unit of the measurement
  end_time: string; // ISO string for the measurement timestamp
};

/**
 * Props for the Info component
 */
type InfoProps = {
  selectedSite: string | null; // Currently selected monitoring site
};

/**
 * Info component displays latest measurement, metadata, and a D3 line chart
 */
function Info({ selectedSite }: InfoProps) {
  const [data, setData] = useState<Measurement[]>([]); // Measurements array
  const [loading, setLoading] = useState(false); // Loading state
  const svgRef = useRef<SVGSVGElement | null>(null); // Reference to D3 SVG
  const containerRef = useRef<HTMLDivElement | null>(null); // Wrapper ref

  // Margin object for D3 chart
  const graphMargin = { top: 10, right: 20, bottom: 20, left: 40 };

  /**
   * Returns a color based on measurement value
   * @param value Number or null
   */
  const getPointColor = (value: number | null) => {
    if (value === null) return "#999"; // Grey if null
    if (value <= 10) return "#2ca02c"; // Green
    if (value <= 20) return "#ff7f0e"; // Orange
    if (value <= 25) return "#ff0000"; // Red
    if (value <= 50) return "#800080"; // Purple
    if (value <= 75) return "#4b0082"; // Indigo
    return "#6a0dad"; // Dark purple
  };

  /**
   * Fetch data whenever a new site is selected
   */
  useEffect(() => {
    if (!selectedSite) return;

    const fetchData = async () => {
      setLoading(true);
      const timeZone = "Europe/Paris";

      const now = new Date();
      now.setMinutes(0, 0, 0); // Round to the top of the hour
      const sevenHoursAgo = new Date(now.getTime() - 8 * 60 * 60 * 1000); // Last 8 hours

      const startStr = formatInTimeZone(
        sevenHoursAgo,
        timeZone,
        "yyyy-MM-dd HH:mm"
      );
      const endStr = formatInTimeZone(now, timeZone, "yyyy-MM-dd HH:mm");

      const url = `http://localhost:8000/measurements/?start_time=${encodeURIComponent(
        startStr
      )}&end_time=${encodeURIComponent(endStr)}&site_name=${encodeURIComponent(
        selectedSite
      )}`;

      try {
        const response = await fetch(url);
        const result = await response.json();
        setData(result); // Update measurements
      } catch (err) {
        console.error(err);
        setData([]);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedSite]);

  /**
   * Get last measurement, fallback to null values
   */
  const lastMeasurement = data[data.length - 1] ?? {
    id: 0,
    pollutant: "",
    raw_value: null,
    unit: "",
    end_time: "",
  };

  // ---- D3 rendering ----
  useEffect(() => {
    if (!data.length || !svgRef.current || !containerRef.current) return;

    const svg = d3.select(svgRef.current);
    const container = svgRef.current.parentElement!;

    const draw = () => {
      const width = container.clientWidth;
      const height = container.clientHeight;
      svg.selectAll("*").remove(); // Clear previous drawings

      // Prepare data: parse times
      const parsed = data.map((d) => ({
        time: new Date(d.end_time),
        value: d.raw_value,
      }));

      // X scale (time)
      const x = d3
        .scaleTime()
        .domain(d3.extent(parsed, (d) => d.time) as [Date, Date])
        .range([graphMargin.left, width - graphMargin.right]);

      // Y scale (value)
      const yValues = parsed
        .map((d) => d.value)
        .filter((v): v is number => v !== null);
      const yMin = d3.min(yValues) ?? 0;
      const yMax = d3.max(yValues) ?? 1;

      const y = d3
        .scaleLinear()
        .domain([yMin, yMax])
        .nice()
        .range([height - graphMargin.bottom, graphMargin.top]);

      svg.attr("viewBox", `0 0 ${width} ${height}`);

      const xTicks = Math.floor(width / 80);
      const yTicks = Math.floor(height / 30);

      // ---- Axes ----
      // X Axis
      const xAxis = d3
        .axisBottom(x)
        .ticks(xTicks)
        .tickFormat(d3.timeFormat("%H:%M") as any)
        .tickSize(0);
      const xAxisGroup = svg
        .append("g")
        .attr("class", "x-axis")
        .attr("transform", `translate(0,${height - graphMargin.bottom})`)
        .call(xAxis);
      xAxisGroup.selectAll(".tick line").remove();

      // Y Axis
      const yAxis = d3.axisLeft(y).ticks(yTicks).tickSize(0);
      const yAxisGroup = svg
        .append("g")
        .attr("class", "y-axis")
        .attr("transform", `translate(${graphMargin.left},0)`)
        .call(yAxis);
      yAxisGroup.selectAll(".tick line").remove();
      yAxisGroup
        .selectAll(".tick text")
        .filter((d) => d === yMin)
        .remove();

      // ---- Grid lines ----
      svg
        .append("g")
        .attr("class", "grid")
        .call((g) =>
          g
            .selectAll("line.grid-line")
            .data(y.ticks(yTicks))
            .join("line")
            .attr("class", "grid-line")
            .attr("x1", graphMargin.left)
            .attr("x2", width - graphMargin.right)
            .attr("y1", (d) => y(d))
            .attr("y2", (d) => y(d))
        );

      // ---- Data lines ----
      const defs = svg.append("defs");
      for (let i = 0; i < parsed.length - 1; i++) {
        const p1 = parsed[i];
        const p2 = parsed[i + 1];
        if (p1.value === null || p2.value === null) continue;

        const color1 = getPointColor(p1.value);
        const color2 = getPointColor(p2.value);

        if (color1 === color2) {
          svg
            .append("line")
            .attr("class", "data-line")
            .attr("x1", x(p1.time))
            .attr("y1", y(p1.value))
            .attr("x2", x(p2.time))
            .attr("y2", y(p2.value))
            .attr("stroke", color1);
        } else {
          const gradId = `grad-${i}`;
          const grad = defs
            .append("linearGradient")
            .attr("id", gradId)
            .attr("gradientUnits", "userSpaceOnUse")
            .attr("x1", x(p1.time))
            .attr("y1", y(p1.value))
            .attr("x2", x(p2.time))
            .attr("y2", y(p2.value));
          grad.append("stop").attr("offset", "0%").attr("stop-color", color1);
          grad.append("stop").attr("offset", "100%").attr("stop-color", color2);
          svg
            .append("line")
            .attr("class", "data-line")
            .attr("x1", x(p1.time))
            .attr("y1", y(p1.value))
            .attr("x2", x(p2.time))
            .attr("y2", y(p2.value))
            .attr("stroke", `url(#${gradId})`);
        }
      }

      // ---- Points ----
      svg
        .selectAll("circle")
        .data(parsed.filter((d) => d.value !== null))
        .join("circle")
        .attr("class", "data-point")
        .attr("cx", (d) => x(d.time))
        .attr("cy", (d) => y(d.value!))
        .attr("r", 4)
        .attr("fill", (d) => getPointColor(d.value));
    };

    draw();
    const resizeObserver = new ResizeObserver(draw);
    resizeObserver.observe(container);

    return () => resizeObserver.disconnect();
  }, [data, graphMargin]);

  // ---- Render ----
  return (
    <div
      ref={containerRef}
      className="section frosted-glass w-full flex flex-col h-full"
    >
      <h1>Info for {selectedSite ?? "—"}</h1>

      {!selectedSite ? (
        // Prompt user to select a station
        <div className="flex-1 flex items-center justify-center text-gray-500 text-lg">
          Please select a station
        </div>
      ) : loading ? (
        // Loading state
        <div className="flex-1 flex items-center justify-center text-gray-400 text-lg">
          Loading data...
        </div>
      ) : (
        // Data display
        <div className="info-content">
          <div className="flex-1 flex flex-col items-center justify-center">
            {/* Latest measurement value */}
            <span
              className="text-5xl font-bold"
              style={{ color: getPointColor(lastMeasurement.raw_value) }}
            >
              {lastMeasurement.raw_value ?? "—"}
            </span>

            {/* Measurement unit and pollutant */}
            <span className="text-sm text-gray-400">
              {lastMeasurement.unit} of {lastMeasurement.pollutant}
            </span>

            {/* Timestamp */}
            {lastMeasurement.end_time && (
              <span className="text-xs text-gray-400 mt-1">
                {`${new Date(lastMeasurement.end_time).toLocaleDateString(
                  "en-GB",
                  { day: "numeric", month: "long", year: "numeric" }
                )} at ${new Date(lastMeasurement.end_time).toLocaleTimeString(
                  "en-GB",
                  { hour: "2-digit", minute: "2-digit" }
                )}`}
              </span>
            )}
          </div>

          {/* D3 graph */}
          <div className="graph">
            <svg ref={svgRef} className="w-full h-full" />
          </div>
        </div>
      )}
    </div>
  );
}

export default Info;
