import React, { useState, useEffect, useCallback } from "react"
import { Link } from "react-router-dom"
import UserDataList from "./UserDataList"
import FilterPanel from "./FilterPanel"
import Pagination from "./Pagination"
import MessageButton from "./MessageButton"

function UserProfile() {
    const [filters, setFilters] = useState({
        segMents: [],
    })
    const [userData, setUserData] = useState([])
    const [totalUsers, setTotalUsers] = useState(0)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState(null)
    const [pagination, setPagination] = useState({
        page: 1,
        size: 10,
    })
    const [segmentNameMap, setSegmentNameMap] = useState({})

    useEffect(() => {
        // Chỉ gọi 1 lần khi component mount
        const fetchSegments = async () => {
            try {
                const res = await fetch("http://localhost:8888/segments/list");
                if (!res.ok) throw new Error(`API segment error: ${res.status}`);
                const segment_data = await res.json();
                const validSegments = segment_data.filter(segment => segment.segment_id != null);
                // Create segment ID to name map
                const nameMap = validSegments.reduce((map, segment) => ({
                    ...map,
                    [segment.segment_id]: segment.segment_name,
                }), {});
                setSegmentNameMap(nameMap);
            } catch (err) {
                console.error("Error fetching segments:", err);
            }
        };
        fetchSegments();
    }, []);


    const fetchData = useCallback(async () => {
        setLoading(true)
        setError(null)
        try {
            if (filters.userId && filters.userId.trim() !== "") {
                // Nếu có userId, gọi API lấy 1 user cụ thể
                const response = await fetch(`http://localhost:8888/user-profile/${filters.userId.trim()}`)
                if (!response.ok) {
                    if (response.status === 404) {
                        setUserData([])
                        setTotalUsers(0)
                        setError("Không tìm thấy user với ID này.")
                        return
                    }
                    throw new Error(`API user-profile error: ${response.status}`)
                }
                const data = await response.json()
                setUserData(data ? [data] : [])
                setTotalUsers(data ? 1 : 0)
            } else {
                const params = new URLSearchParams()
                params.append("page", pagination.page)
                params.append("size", pagination.size)
                if (filters.segMents && filters.segMents.length > 0) {
                    filters.segMents.forEach(segmentId => params.append("segment_id", segmentId))
                }
                const response = await fetch(`http://localhost:8888/user-profiles?${params.toString()}`)
                if (!response.ok) {
                    throw new Error(`API user-profiles error: ${response.status}`)
                }
                const data = await response.json()
                setUserData(data.data || [])
                setTotalUsers(data.totalSize || 0)
            }
        } catch (err) {
            console.error("Error fetching data:", err)
            setError("Không thể tải dữ liệu. Vui lòng thử lại sau.")
        } finally {
            setLoading(false)
        }
    }, [filters, pagination])

    useEffect(() => {
        fetchData()
    }, [fetchData])

    const handleFilterChange = (name, value) => {
        setFilters((prev) => ({
            ...prev,
            [name]: value,
        }))
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
            page: 1,
        }))
    }

    return (
        <div className="min-h-screen bg-gradient-to-b from-gray-50 to-gray-100 p-4 md:p-8">
            <div className="max-w-7xl mx-auto">
                <div className="flex items-center justify-between mb-6">
                    <h1 className="text-3xl font-bold text-gray-800">User Profile Dashboard</h1>
                    <Link
                        to="/"
                        className="flex items-center px-4 py-2 rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors"
                    >
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-5 w-5 mr-2"
                            viewBox="0 0 20 20"
                            fill="currentColor"
                        >
                        </svg>
                        Trang chủ
                    </Link>
                </div>

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
                                <UserDataList userData={userData} segmentNameMap={segmentNameMap} />
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
        </div>
    )
}

export default UserProfile