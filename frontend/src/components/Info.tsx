import { useEffect, useRef, useState } from "react";
import { formatInTimeZone } from "date-fns-tz";
import * as d3 from "d3";

type Measurement = {
  id: number;
  pollutant: string;
  raw_value: number | null;
  unit: string;
  end_time: string;
};

type InfoProps = {
  selectedSite: string | null;
};

function Info({ selectedSite }: InfoProps) {
  const [data, setData] = useState<Measurement[]>([]);
  const [loading, setLoading] = useState(false);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const graphMargin = {
    top: 10,
    right: 20, // ← tu peux modifier ici
    bottom: 20,
    left: 30, // ← et ici
  };

  const getPointColor = (value: number | null) => {
    if (value === null) return "#999";
    if (value <= 10) return "#2ca02c";
    if (value <= 20) return "#ff7f0e";
    if (value <= 25) return "#ff0000";
    if (value <= 50) return "#800080";
    if (value <= 75) return "#4b0082";
    return "#6a0dad";
  };

  useEffect(() => {
    if (!selectedSite) return;

    const fetchData = async () => {
      setLoading(true);
      const timeZone = "Europe/Paris";
      const now = new Date();
      now.setMinutes(0, 0, 0); // → arrondi à l'heure pleine
      const sevenHoursAgo = new Date(now.getTime() - 8 * 60 * 60 * 1000);

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
      console.log(url);

      try {
        const response = await fetch(url);
        const result = await response.json();
        setData(result);
      } catch (err) {
        console.error(err);
        setData([]);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedSite]);

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
      svg.selectAll("*").remove();

      const parsed = data.map((d) => ({
        time: new Date(d.end_time),
        value: d.raw_value,
      }));

      const x = d3
        .scaleTime()
        .domain(d3.extent(parsed, (d) => d.time) as [Date, Date])
        .range([graphMargin.left, width - graphMargin.right]);

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

      svg
        .append("g")
        .attr("transform", `translate(0,${height - graphMargin.bottom})`)
        .call(
          d3
            .axisBottom(x)
            .ticks(xTicks)
            .tickFormat(d3.timeFormat("%H:%M") as any)
        );

      svg
        .append("g")
        .attr("transform", `translate(${graphMargin.left},0)`)
        .call(d3.axisLeft(y).ticks(yTicks));

      svg
        .append("g")
        .attr("stroke-opacity", 0.1)
        .attr("shape-rendering", "crispEdges")
        .call((g) =>
          g
            .selectAll("line.horizontal")
            .data(y.ticks(yTicks))
            .join("line")
            .attr("x1", graphMargin.left)
            .attr("x2", width - graphMargin.right)
            .attr("y1", (d) => y(d))
            .attr("y2", (d) => y(d))
        );

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
            .attr("x1", x(p1.time))
            .attr("y1", y(p1.value))
            .attr("x2", x(p2.time))
            .attr("y2", y(p2.value))
            .attr("stroke-width", 3)
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
            .attr("x1", x(p1.time))
            .attr("y1", y(p1.value))
            .attr("x2", x(p2.time))
            .attr("y2", y(p2.value))
            .attr("stroke-width", 3)
            .attr("stroke", `url(#${gradId})`);
        }
      }

      svg
        .selectAll("circle")
        .data(parsed.filter((d) => d.value !== null))
        .join("circle")
        .attr("cx", (d) => x(d.time))
        .attr("cy", (d) => y(d.value!)) // on sait que value n'est pas null ici
        .attr("r", 4)
        .attr("fill", (d) => getPointColor(d.value));
    };

    draw();
    const resizeObserver = new ResizeObserver(draw);
    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, [data, graphMargin]);

  return (
    <div
      ref={containerRef}
      className="bg-white flex flex-col h-auto lg:h-full lg:w-[30%] p-4 gap-2 rounded-xl shadow-xl"
    >
      <h1>Info for {selectedSite ?? "—"}</h1>

      {!selectedSite ? (
        <div className="flex-1 flex items-center justify-center text-gray-500 text-lg">
          Please select a station
        </div>
      ) : (
        <div className="flex flex-row lg:flex-col flex-1 gap-8 lg:gap-4">
          {/* Dernière mesure */}
          <div className="flex-1 flex flex-col items-center justify-center">
            <span className="text-sm text-gray-500"></span>
            <span
              className="text-5xl font-bold"
              style={{ color: getPointColor(lastMeasurement.raw_value) }}
            >
              {lastMeasurement.raw_value ?? "—"}
            </span>
            <span className="text-sm text-gray-400">
              {lastMeasurement.unit} of {lastMeasurement.pollutant}
            </span>
            {/* Nouvelle ligne pour la date/heure */}
            {/* Date reformattée */}
            {lastMeasurement.end_time && (
              <span className="text-xs text-gray-400 mt-1">
                {`${new Date(lastMeasurement.end_time).toLocaleDateString(
                  "en-GB",
                  {
                    day: "numeric",
                    month: "long",
                    year: "numeric",
                  }
                )} at ${new Date(lastMeasurement.end_time).toLocaleTimeString(
                  "en-GB",
                  {
                    hour: "2-digit",
                    minute: "2-digit",
                  }
                )}`}
              </span>
            )}
          </div>

          {/* Graphe */}
          <div className="flex-4 w-full pr-4 min-h-[200px] md:min-h-[250px] lg:h-full">
            <svg ref={svgRef} className="w-full h-full" />
          </div>
        </div>
      )}
    </div>
  );
}

export default Info;
