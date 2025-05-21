"use client"

import { useState } from "react"
import PropTypes from "prop-types"

function Pagination({ currentPage, pageSize, totalItems, onPageChange, onPageSizeChange }) {
    const totalPages = Math.ceil(totalItems / pageSize)
    const [inputPage, setInputPage] = useState(currentPage.toString())
    const [isInputFocused, setIsInputFocused] = useState(false)
    const handlePageChange = (page) => {
        onPageChange(page)
    }
    // Generate page numbers to display
    const getPageNumbers = () => {
        const pages = []
        const maxPagesToShow = 5 // Show at most 5 page numbers

        if (totalPages <= maxPagesToShow) {
            // If we have 5 or fewer pages, show all of them
            for (let i = 1; i <= totalPages; i++) {
                pages.push(i)
            }
        } else {
            // Always show first page
            pages.push(1)

            // Calculate start and end of page numbers to show
            let start = Math.max(2, currentPage - 1)
            let end = Math.min(totalPages - 1, currentPage + 1)

            // Adjust if we're at the beginning or end
            if (currentPage <= 2) {
                end = 4
            } else if (currentPage >= totalPages - 1) {
                start = totalPages - 3
            }

            // Add ellipsis if needed
            if (start > 2) {
                pages.push("...")
            }

            // Add middle pages
            for (let i = start; i <= end; i++) {
                pages.push(i)
            }

            // Add ellipsis if needed
            if (end < totalPages - 1) {
                pages.push("...")
            }

            // Always show last page
            if (totalPages > 1) {
                pages.push(totalPages)
            }
        }

        return pages
    }

    const handlePageInputChange = (e) => {
        setInputPage(e.target.value)
    }

    const handlePageInputBlur = () => {
        setIsInputFocused(false)

        // Validate and navigate to the input page
        const pageNum = parseInt(inputPage, 10)
        if (!isNaN(pageNum) && pageNum >= 1 && pageNum <= totalPages) {
            onPageChange(pageNum)
        } else {
            // Reset to current page if invalid
            setInputPage(currentPage.toString())
        }
    }

    const handlePageInputKeyDown = (e) => {
        if (e.key === "Enter") {
            e.target.blur() // Trigger blur event to handle navigation
        }
    }

    // If there's only one page or no items, don't show pagination
    if (totalPages <= 1 || totalItems === 0) return null

    return (
        <div className="mt-6 bg-white rounded-lg shadow-md p-4">
            <div className="flex flex-col sm:flex-row justify-between items-center gap-4">
                <div className="flex items-center space-x-2">
                    <span className="text-sm text-gray-600">Hiển thị</span>
                    <select
                        className="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                        value={pageSize}
                        onChange={(e) => onPageSizeChange && onPageSizeChange(Number(e.target.value))}
                    >
                        <option value="5">5</option>
                        <option value="10">10</option>
                        <option value="20">20</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                    </select>
                    <span className="text-sm text-gray-600">mục / trang</span>

                    <span className="text-sm text-gray-600 ml-4">Đến trang</span>
                    <div className="relative">
                        <input
                            type="text"
                            value={isInputFocused ? inputPage : currentPage}
                            onChange={handlePageInputChange}
                            onFocus={() => {
                                setIsInputFocused(true)
                                setInputPage(currentPage.toString())
                            }}
                            onBlur={handlePageInputBlur}
                            onKeyDown={handlePageInputKeyDown}
                            className="w-16 px-2 py-1 border border-gray-300 rounded-md text-center text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                            aria-label="Nhập số trang"
                        />
                    </div>
                    <span className="text-sm text-gray-600">/ {totalPages}</span>
                </div>

                <div className="flex items-center space-x-1">
                    <button
                        onClick={() => onPageChange(1)}
                        disabled={currentPage === 1}
                        className={`px-2 py-1 rounded-md ${currentPage === 1
                            ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                            : "bg-white text-gray-700 hover:bg-gray-50"
                            }`}
                        title="Trang đầu"
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M15.707 15.707a1 1 0 01-1.414 0l-5-5a1 1 0 010-1.414l5-5a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 010 1.414z"
                                clipRule="evenodd"
                            />
                            <path
                                fillRule="evenodd"
                                d="M9.707 15.707a1 1 0 01-1.414 0l-5-5a1 1 0 010-1.414l5-5a1 1 0 111.414 1.414L5.414 10l4.293 4.293a1 1 0 010 1.414z"
                                clipRule="evenodd"
                            />
                        </svg>
                    </button>

                    <button
                        onClick={() => onPageChange(currentPage - 1)}
                        disabled={currentPage === 1}
                        className={`px-2 py-1 rounded-md ${currentPage === 1
                            ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                            : "bg-white text-gray-700 hover:bg-gray-50"
                            }`}
                        title="Trang trước"
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M12.707 5.293a1 1 0 010 1.414L9.414 10l3.293 3.293a1 1 0 01-1.414 1.414l-4-4a1 1 0 010-1.414l4-4a1 1 0 011.414 0z"
                                clipRule="evenodd"
                            />
                        </svg>
                    </button>

                    {/* Page numbers */}
                    <div className="hidden md:flex items-center space-x-1">
                        {getPageNumbers().map((page, index) => (
                            <button
                                key={index}
                                onClick={() => handlePageChange(page)}
                                disabled={page === "..."}
                                className={`px-3 py-1 rounded-md ${page === currentPage
                                    ? "bg-blue-600 text-white"
                                    : page === "..."
                                        ? "bg-white text-gray-700 cursor-default"
                                        : "bg-white text-gray-700 hover:bg-gray-50"
                                    }`}
                            >
                                {page}
                            </button>
                        ))}
                    </div>

                    <button
                        onClick={() => onPageChange(currentPage + 1)}
                        disabled={currentPage === totalPages}
                        className={`px-2 py-1 rounded-md ${currentPage === totalPages
                            ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                            : "bg-white text-gray-700 hover:bg-gray-50"
                            }`}
                        title="Trang sau"
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z"
                                clipRule="evenodd"
                            />
                        </svg>
                    </button>

                    <button
                        onClick={() => onPageChange(totalPages)}
                        disabled={currentPage === totalPages}
                        className={`px-2 py-1 rounded-md ${currentPage === totalPages
                            ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                            : "bg-white text-gray-700 hover:bg-gray-50"
                            }`}
                        title="Trang cuối"
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M4.293 15.707a1 1 0 001.414 0l5-5a1 1 0 000-1.414l-5-5a1 1 0 00-1.414 1.414L8.586 10 4.293 14.293a1 1 0 000 1.414z"
                                clipRule="evenodd"
                            />
                            <path
                                fillRule="evenodd"
                                d="M10.293 15.707a1 1 0 001.414 0l5-5a1 1 0 000-1.414l-5-5a1 1 0 00-1.414 1.414L14.586 10l-4.293 4.293a1 1 0 000 1.414z"
                                clipRule="evenodd"
                            />
                        </svg>
                    </button>
                </div>
            </div>

            <div className="mt-2 text-center text-sm text-gray-600">
                Hiển thị {Math.min((currentPage - 1) * pageSize + 1, totalItems)} đến{" "}
                {Math.min(currentPage * pageSize, totalItems)} trong tổng số {totalItems} mục
            </div>
        </div>
    )
}

Pagination.propTypes = {
    currentPage: PropTypes.number.isRequired,
    pageSize: PropTypes.number.isRequired,
    totalItems: PropTypes.number.isRequired,
    onPageChange: PropTypes.func.isRequired,
    onPageSizeChange: PropTypes.func,
}

export default Pagination