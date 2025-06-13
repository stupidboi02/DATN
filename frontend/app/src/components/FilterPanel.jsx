"use client"
import { useState, useEffect } from "react"
import PropTypes from "prop-types"

function FilterPanel({ filters, onFilterChange }) {
    const [isCollapsed, setIsCollapsed] = useState(false)
    const [segments, setSegments] = useState([])
    const [isLoadingSegments, setIsLoadingSegments] = useState(false)
    const [segmentError, setSegmentError] = useState("")
    const [userId, setUserId] = useState("")

    useEffect(() => {
        const fetchSegments = async () => {
            setIsLoadingSegments(true)
            setSegmentError("")
            try {
                const response = await fetch("http://localhost:8888/segments/list")
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`)
                }
                const data = await response.json()
                setSegments(data.filter(segment => segment.segment_id != null))
            } catch (err) {
                setSegmentError(`Failed to fetch segments: ${err.message}`)
            } finally {
                setIsLoadingSegments(false)
            }
        }
        fetchSegments()
    }, [])

    const handleSubmit = (e) => {
        e.preventDefault()
        // Chỉ lọc theo segment
    }

    return (
        <div className="bg-white rounded-lg shadow-md p-4">
            <div className="flex justify-between items-center mb-4">
                <h2 className="text-xl font-semibold text-gray-800">Bộ lọc</h2>
                <button
                    onClick={() => setIsCollapsed(!isCollapsed)}
                    className="text-gray-500 hover:text-gray-700"
                    type="button"
                >
                    {isCollapsed ? "Mở rộng" : "Thu gọn"}
                </button>
            </div>

            {!isCollapsed && (
                <form onSubmit={handleSubmit} className="space-y-4">
                    <div>
                        <label htmlFor="userId" className="block text-sm font-medium text-gray-700 mb-1">User ID</label>
                        <input
                            type="text"
                            id="userId"
                            name="userId"
                            value={userId}
                            onChange={e => {
                                setUserId(e.target.value);
                                onFilterChange("userId", e.target.value);
                            }}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 mb-2"
                            placeholder="Nhập user_id cụ thể"
                        />
                    </div>
                    <div>
                        <label htmlFor="segMents" className="block text-sm font-medium text-gray-700 mb-1">Phân Khúc</label>
                        <select
                            id="segMents"
                            multiple
                            value={filters.segMents || []}
                            onChange={(e) => {
                                const selected = Array.from(e.target.selectedOptions, option => option.value)
                                onFilterChange("segMents", selected)
                            }}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            style={{ minHeight: "90px" }}
                            disabled={isLoadingSegments}
                        >
                            {segments.map((segment) => (
                                <option key={segment.segment_id} value={segment.segment_id}>
                                    {segment.segment_name}
                                </option>
                            ))}
                        </select>
                        {segmentError && (
                            <p className="mt-1 text-sm text-red-600">{segmentError}</p>
                        )}
                        {isLoadingSegments && (
                            <p className="mt-1 text-sm text-gray-600">Đang tải phân khúc...</p>
                        )}
                    </div>
                    <button
                        type="button"
                        onClick={() => {
                            setUserId("");
                            onFilterChange("userId", "");
                            onFilterChange("segMents", []);
                        }}
                        className="w-full mt-4 bg-gray-200 text-gray-700 py-2 px-4 rounded-md hover:bg-gray-300 transition-colors"
                    >
                        Xóa bộ lọc
                    </button>
                </form>
            )}
        </div>
    )
}

FilterPanel.propTypes = {
    filters: PropTypes.shape({
        segMents: PropTypes.arrayOf(PropTypes.string),
    }).isRequired,
    onFilterChange: PropTypes.func.isRequired,
}

export default FilterPanel