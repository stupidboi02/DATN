"use client"

import { useState, useEffect } from "react"
import PropTypes from "prop-types"

function MessageModal({ isOpen, onClose, onSend, selectedUsers, isBatchMessage, filters }) {
    const [subject, setSubject] = useState("")
    const [content, setContent] = useState("")
    const [messageType, setMessageType] = useState("notification")
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState(null)
    const [previewCount, setPreviewCount] = useState(0)

    useEffect(() => {
        // Reset form when modal opens
        if (isOpen) {
            setSubject("")
            setContent("")
            setMessageType("notification")
            setError(null)

            // If it's a batch message, fetch preview count
            if (isBatchMessage && filters) {
                fetchPreviewCount()
            }
        }
    }, [isOpen, isBatchMessage, filters])

    const fetchPreviewCount = async () => {
        try {
            // Build query parameters from filters
            const params = new URLSearchParams()
            if (filters.segMents) {
                params.append("segments", filters.segMents)
            }
            // Set page size to 1 just to get total count
            params.append("page", 1)
            params.append("size", 1)
            const response = await fetch(`http://127.0.0.1:8888//user-profiles?${params.toString()}`)
            if (!response.ok) {
                throw new Error(`API error: ${response.status}`)
            }
            const data = await response.json()
            setPreviewCount(data.totalSize || 0)
        } catch (err) {
            console.error("Error fetching preview count:", err)
            setError("Không thể tải số lượng người nhận. Vui lòng thử lại sau.")
        }
    }

    const handleSubmit = async (e) => {
        e.preventDefault()

        if (!subject.trim() || !content.trim()) {
            setError("Vui lòng nhập cả tiêu đề và nội dung tin nhắn")
            return
        }

        setIsLoading(true)
        setError(null)

        try {
            let result

            if (isBatchMessage) {
                // Convert filters to the format expected by the API
                const apiFilters = {
                    user_ids: filters.userId ? [Number.parseInt(filters.userId)] : undefined,
                    segments: filters.segMents || undefined,
                }

                result = await onSend({
                    subject,
                    content,
                    message_type: messageType,
                    filters: apiFilters,
                })
            } else {
                // Single user or selected users
                if (selectedUsers && selectedUsers.length > 0) {
                    // Send to each selected user
                    const results = []
                    for (const user of selectedUsers) {
                        const userResult = await onSend({
                            user_id: user.user_id,
                            subject,
                            content,
                            message_type: messageType,
                        })
                        results.push(userResult)
                    }
                    result = {
                        sent_count: results.reduce((sum, r) => sum + (r.sent_count || 0), 0),
                        failed_count: results.reduce((sum, r) => sum + (r.failed_count || 0), 0),
                    }
                } else {
                    setError("Không có người dùng nào được chọn")
                    setIsLoading(false)
                    return
                }
            }

            // Success - close modal
            onClose(result)
        } catch (err) {
            console.error("Error sending message:", err)
            setError(`Lỗi gửi tin nhắn: ${err.message || "Vui lòng thử lại sau"}`)
        } finally {
            setIsLoading(false)
        }
    }

    if (!isOpen) return null

    return (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
            <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
                <div className="p-6">
                    <div className="flex justify-between items-center mb-4">
                        <h2 className="text-xl font-semibold text-gray-800">
                            {isBatchMessage ? "Gửi tin nhắn hàng loạt" : "Gửi tin nhắn"}
                        </h2>
                        <button onClick={() => onClose()} className="text-gray-500 hover:text-gray-700" aria-label="Đóng">
                            <svg
                                xmlns="http://www.w3.org/2000/svg"
                                className="h-6 w-6"
                                fill="none"
                                viewBox="0 0 24 24"
                                stroke="currentColor"
                            >
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                            </svg>
                        </button>
                    </div>

                    {isBatchMessage && (
                        <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                            <p className="text-blue-800 flex items-center">
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    className="h-5 w-5 mr-2"
                                    viewBox="0 0 20 20"
                                    fill="currentColor"
                                >
                                    <path
                                        fillRule="evenodd"
                                        d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
                                        clipRule="evenodd"
                                    />
                                </svg>
                                Tin nhắn sẽ được gửi đến {previewCount} người dùng phù hợp với bộ lọc hiện tại.
                            </p>
                        </div>
                    )}

                    {!isBatchMessage && selectedUsers && (
                        <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                            <p className="text-blue-800">Gửi tin nhắn đến {selectedUsers.length} người dùng đã chọn:</p>
                            <div className="mt-2 max-h-20 overflow-y-auto">
                                <ul className="text-sm text-blue-700">
                                    {selectedUsers.map((user) => (
                                        <li key={user.user_id} className="mb-1">
                                            User ID: {user.user_id}
                                            {user.segments && ` - ${user.segments}`}
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        </div>
                    )}

                    {error && (
                        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-md">
                            <p className="text-red-800 flex items-center">
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    className="h-5 w-5 mr-2"
                                    viewBox="0 0 20 20"
                                    fill="currentColor"
                                >
                                    <path
                                        fillRule="evenodd"
                                        d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z"
                                        clipRule="evenodd"
                                    />
                                </svg>
                                {error}
                            </p>
                        </div>
                    )}

                    <form onSubmit={handleSubmit}>
                        <div className="mb-4">
                            <label htmlFor="messageType" className="block text-sm font-medium text-gray-700 mb-1">
                                Loại tin nhắn
                            </label>
                            <select
                                id="messageType"
                                value={messageType}
                                onChange={(e) => setMessageType(e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            >
                                <option value="notification">Thông báo</option>
                                <option value="promotion">Khuyến mãi</option>
                                <option value="alert">Cảnh báo</option>
                                <option value="update">Cập nhật</option>
                            </select>
                        </div>

                        <div className="mb-4">
                            <label htmlFor="subject" className="block text-sm font-medium text-gray-700 mb-1">
                                Tiêu đề
                            </label>
                            <input
                                type="text"
                                id="subject"
                                value={subject}
                                onChange={(e) => setSubject(e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Nhập tiêu đề tin nhắn"
                                required
                            />
                        </div>

                        <div className="mb-4">
                            <label htmlFor="content" className="block text-sm font-medium text-gray-700 mb-1">
                                Nội dung
                            </label>
                            <textarea
                                id="content"
                                value={content}
                                onChange={(e) => setContent(e.target.value)}
                                rows={6}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                placeholder="Nhập nội dung tin nhắn"
                                required
                            ></textarea>
                            <p className="mt-1 text-sm text-gray-500">
                                {/* Bạn có thể sử dụng các biến như {"{name}"} để cá nhân hóa tin nhắn. */}
                            </p>
                        </div>

                        <div className="flex justify-end space-x-3">
                            <button
                                type="button"
                                onClick={() => onClose()}
                                className="px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
                                disabled={isLoading}
                            >
                                Hủy
                            </button>
                            <button
                                type="submit"
                                className="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
                                disabled={isLoading}
                            >
                                {isLoading ? (
                                    <span className="flex items-center">
                                        <svg
                                            className="animate-spin -ml-1 mr-2 h-4 w-4 text-white"
                                            xmlns="http://www.w3.org/2000/svg"
                                            fill="none"
                                            viewBox="0 0 24 24"
                                        >
                                            <circle
                                                className="opacity-25"
                                                cx="12"
                                                cy="12"
                                                r="10"
                                                stroke="currentColor"
                                                strokeWidth="4"
                                            ></circle>
                                            <path
                                                className="opacity-75"
                                                fill="currentColor"
                                                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                                            ></path>
                                        </svg>
                                        Đang gửi...
                                    </span>
                                ) : (
                                    "Gửi tin nhắn"
                                )}
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    )
}

MessageModal.propTypes = {
    isOpen: PropTypes.bool.isRequired,
    onClose: PropTypes.func.isRequired,
    onSend: PropTypes.func.isRequired,
    selectedUsers: PropTypes.array,
    isBatchMessage: PropTypes.bool,
    filters: PropTypes.object,
}

export default MessageModal
