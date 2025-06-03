"use client"

import { BrowserRouter as Router, Routes, Route } from "react-router-dom"
import HomePage from "./components/HomePage"
import UserProfile from "./components/UserProfile"
import Dashboard from "./components/Dashboard"
import GetDataPage from "./components/GetDataPage"
import AnalyticsPage from "./components/AnalyticsPage"
import "./App.css"

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/user-profile" element={<UserProfile />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/get-data" element={<GetDataPage />} />
        <Route path="/analytics/:userId" element={<AnalyticsPage />} />
      </Routes>
    </Router>
  )
}

export default App