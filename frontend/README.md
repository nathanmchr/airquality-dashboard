# 🌿 France Air Quality Frontend

A modern, interactive frontend for visualizing air quality measurements
across France.\
Built with **React**, **TypeScript**, **Leaflet**, **D3**, and
**TailwindCSS**, the application displays real-time pollutant trends and
monitoring station details.

---

## 📘 Before you begin

If you plan to install the full project using **Docker**, please follow
the main documentation first:

👉 **See main setup documentation**:\
[Main documentation](../README.md)

This README describes:

✅ how to run the frontend manually **without Docker**,\
✅ how to develop locally using Vite.

---

# 🐳 Setup With Docker (Recommended)

If you are running the full project using Docker (API + ETL + Frontend +
Database), the frontend will be built and served automatically.

---

# 🖥️ Setup Without Docker (Manual Run)

Useful during development, debugging, or UI-only testing.

---

## 1. Install dependencies

From the `frontend/` folder:

    npm install

or

    yarn install

---

## 2. API Configuration

The frontend expects the backend API to run at:

    http://127.0.0.1:8000

If your API runs somewhere else, update the base URL in:

    src/apiCalls.ts

---

## 3. Start development server

    npm run dev

or

    yarn dev

Then open the browser using the development URL displayed in the
terminal.

---

# 🎨 Features

✅ Interactive Map
✅ Info Panel
✅ Legend & Tooltips
✅ Responsive UI (dark mode + frosted glass)

---

# 🛠️ Tech Stack

- React
- TypeScript
- Leaflet + React-Leaflet
- D3.js
- TailwindCSS
- date-fns-tz

---

# 📄 License

GPL-3.0 license © 2025 --- France Air Quality Dashboard
