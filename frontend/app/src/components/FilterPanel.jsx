"use client"

import { useState } from "react"
import PropTypes from "prop-types"

function FilterPanel({ filters, onFilterChange }) {
    const [isCollapsed, setIsCollapsed] = useState(false)

    const handleSubmit = (e) => {
        e.preventDefault()
        // Form submission is handled by the input change events
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
                        <label htmlFor="userId" className="block text-sm font-medium text-gray-700 mb-1">
                            User ID
                        </label>
                        <input
                            type="text"
                            id="userId"
                            value={filters.userId}
                            onChange={(e) => onFilterChange("userId", e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            placeholder="Nhập User ID"
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Tổng số lần ghé thăm</label>
                        <div className="grid grid-cols-2 gap-2">
                            <input
                                type="number"
                                value={filters.totalVisitsMin}
                                onChange={(e) => onFilterChange("totalVisitsMin", e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Tối thiểu"
                                min="0"
                            />
                            <input
                                type="number"
                                value={filters.totalVisitsMax}
                                onChange={(e) => onFilterChange("totalVisitsMax", e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Tối đa"
                                min="0"
                            />
                        </div>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Tổng chi tiêu</label>
                        <div className="grid grid-cols-2 gap-2">
                            <input
                                type="number"
                                value={filters.totalSpendMin}
                                onChange={(e) => onFilterChange("totalSpendMin", e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Tối thiểu"
                                min="0"
                            />
                            <input
                                type="number"
                                value={filters.totalSpendMax}
                                onChange={(e) => onFilterChange("totalSpendMax", e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Tối đa"
                                min="0"
                            />
                        </div>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Tổng số sản phẩm đã mua</label>
                        <div className="grid grid-cols-2 gap-2">
                            <input
                                type="number"
                                value={filters.totalItemsMin}
                                onChange={(e) => onFilterChange("totalItemsMin", e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Tối thiểu"
                                min="0"
                            />
                            <input
                                type="number"
                                value={filters.totalItemsMax}
                                onChange={(e) => onFilterChange("totalItemsMax", e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Tối đa"
                                min="0"
                            />
                        </div>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Phân Khúc</label>
                        <select
                            value={filters.segMents || ""}
                            onChange={(e) => onFilterChange("segMents", e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                        >
                            <option value="">Tất cả phân khúc</option>
                            <option value="VIP">VIP</option>
                            <option value="High Value">High Value</option>
                            <option value="Recent Active Buyers">Recent Active Buyers</option>
                            <option value="General Audience">General Audience</option>
                        </select>
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Mức độ rủi ro</label>
                        <select
                            value={filters.churnRisk || ""}
                            onChange={(e) => onFilterChange("churnRisk", e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                        >
                            <option value="">Tất cả mức độ rủi ro</option>
                            <option value="Very High">Rất Cao</option>
                            <option value="High">Cao</option>
                            <option value="Normal">Trung Bình</option>
                            <option value="Low">Thấp</option>
                        </select>
                    </div>

                    <button
                        type="button"
                        onClick={() => {
                            onFilterChange("userId", "")
                            onFilterChange("totalVisitsMin", "")
                            onFilterChange("totalVisitsMax", "")
                            onFilterChange("totalSpendMin", "")
                            onFilterChange("totalSpendMax", "")
                            onFilterChange("totalItemsMin", "")
                            onFilterChange("totalItemsMax", "")
                            onFilterChange("segMents", "")
                            onFilterChange("churnRisk", "")
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
        userId: PropTypes.string,
        totalVisitsMin: PropTypes.string,
        totalVisitsMax: PropTypes.string,
        totalSpendMin: PropTypes.string,
        totalSpendMax: PropTypes.string,
        totalItemsMin: PropTypes.string,
        totalItemsMax: PropTypes.string,
        segMents: PropTypes.string,
        churnRisk: PropTypes.string,
    }).isRequired,
    onFilterChange: PropTypes.func.isRequired,
}

export default FilterPanel
