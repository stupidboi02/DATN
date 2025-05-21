"use client"

import { useState, useEffect, useCallback } from "react"
import UserDataList from "./components/UserDataList"
import FilterPanel from "./components/FilterPanel"
import Pagination from "./components/Pagination"
import MessageButton from "./components/MessageButton"
import "./App.css"

function App() {
  // Thay đổi state filters để thêm totalSpendSort nhưng vẫn giữ lại totalSpendMin và totalSpendMax
  const [filters, setFilters] = useState({
    userId: "",
    totalVisitsMin: "",
    totalVisitsMax: "",
    totalSpendMin: "",
    totalSpendMax: "",
    totalSpendSort: "",
    totalItemsMin: "",
    totalItemsMax: "",
    segMents: "",
    churnRisk: "",
  })
  const [userData, setUserData] = useState([])
  const [totalUsers, setTotalUsers] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [pagination, setPagination] = useState({
    page: 1,
    size: 10,
  })

  // Cập nhật hàm fetchData để sử dụng cả tham số lọc và sắp xếp
  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)

    try {
      // Build query parameters
      const params = new URLSearchParams()
      params.append("page", pagination.page)
      params.append("size", pagination.size)

      if (filters.userId) {
        params.append("user_id", filters.userId)
      }
      if (filters.totalVisitsMin) {
        params.append("min_total_visits", filters.totalVisitsMin)
      }
      if (filters.totalVisitsMax) {
        params.append("max_total_visits", filters.totalVisitsMax)
      }
      if (filters.totalSpendMin) {
        params.append("min_total_spend", filters.totalSpendMin)
      }
      if (filters.totalSpendMax) {
        params.append("max_total_spend", filters.totalSpendMax)
      }
      if (filters.totalSpendSort) {
        params.append("sort_by", "total_spend")
        params.append("sort_order", filters.totalSpendSort)
      }
      if (filters.totalItemsMin) {
        params.append("min_total_items_purchased", filters.totalItemsMin)
      }
      if (filters.totalItemsMax) {
        params.append("max_total_items_purchased", filters.totalItemsMax)
      }
      if (filters.segMents) {
        params.append("segments", filters.segMents)
      }
      if (filters.churnRisk) {
        params.append("churn_risk", filters.churnRisk)
      }

      const response = await fetch(`http://127.0.0.1:8000/user-profiles?${params.toString()}`)

      if (!response.ok) {
        throw new Error(`API error: ${response.status}`)
      }

      const data = await response.json()

      // Handle different API response formats
      if (Array.isArray(data)) {
        setUserData(data)
        setTotalUsers(data.length) // This might need adjustment based on actual API response
      } else if (data && typeof data === "object") {
        // If the API returns an object with items and total
        setUserData(data.data || [])
        setTotalUsers(data?.totalSize || data?.data?.length || 0)
      } else {
        throw new Error("Unexpected API response format")
      }
    } catch (err) {
      console.error("Error fetching data:", err)
      setError("Không thể tải dữ liệu. Vui lòng thử lại sau.")
    } finally {
      setLoading(false)
    }
  }, [filters, pagination]) // Include dependencies here

  useEffect(() => {
    fetchData()
  }, [fetchData]) // Now fetchData is the only dependency

  const handleFilterChange = (name, value) => {
    setFilters((prev) => ({
      ...prev,
      [name]: value,
    }))
    // Reset to first page when filters change
    setPagination((prev) => ({
      ...prev,
      page: 1,
    }))
  }

  const handlePageChange = (newPage) => {
    setPagination((prev) => ({
      ...prev,
      page: newPage,
    }))
  }

  const handlePageSizeChange = (newSize) => {
    setPagination((prev) => ({
      ...prev,
      size: newSize,
      page: 1, // Reset to first page when changing page size
    }))
  }

  return (
    <main className="min-h-screen bg-gradient-to-b from-gray-50 to-gray-100 p-4 md:p-8">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-3xl font-bold text-gray-800 mb-6">User Data Dashboard</h1>

        <div className="grid grid-cols-1 lg:grid-cols-[300px_1fr] gap-6">
          <FilterPanel filters={filters} onFilterChange={handleFilterChange} />

          <div>
            <div className="bg-white p-4 rounded-lg shadow-md mb-4 flex justify-between items-center">
              <p className="text-gray-700">
                {loading ? (
                  "Đang tải dữ liệu..."
                ) : (
                  <>
                    Hiển thị <span className="font-semibold">{userData.length}</span> trên tổng số{" "}
                    <span className="font-semibold">{totalUsers}</span> người dùng
                  </>
                )}
              </p>

              {/* Add batch message button */}
              <MessageButton isBatchMessage={true} filters={filters} />
            </div>

            {error ? (
              <div className="bg-red-100 text-red-800 p-4 rounded-lg shadow-md mb-4">
                <p>{error}</p>
                <button onClick={fetchData} className="mt-2 bg-red-200 hover:bg-red-300 text-red-800 px-4 py-2 rounded">
                  Thử lại
                </button>
              </div>
            ) : loading ? (
              <div className="bg-white p-8 rounded-lg shadow-md text-center">
                <div className="loader"></div>
                <p className="mt-4 text-gray-600">Đang tải dữ liệu người dùng...</p>
              </div>
            ) : (
              <>
                <UserDataList userData={userData} />

                {/* Phân trang ở cuối trang */}
                <Pagination
                  currentPage={pagination.page}
                  pageSize={pagination.size}
                  totalItems={totalUsers}
                  onPageChange={handlePageChange}
                  onPageSizeChange={handlePageSizeChange}
                />
              </>
            )}
          </div>
        </div>
      </div>
    </main>
  )
}

export default App
