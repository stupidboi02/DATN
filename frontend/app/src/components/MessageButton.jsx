"use client"

import { useState } from "react"
import PropTypes from "prop-types"
import MessageModal from "./MessageModal"

function MessageButton({ user, filters, isBatchMessage = false }) {
    const [isModalOpen, setIsModalOpen] = useState(false)
    const [isLoading, setIsLoading] = useState(false)
    const [result, setResult] = useState(null)
    const [error, setError] = useState(null)

    const handleSendMessage = async (messageData) => {
        setIsLoading(true)
        setError(null)

        try {
            let endpoint
            let payload

            if (isBatchMessage) {
                endpoint = "http://127.0.0.1:8000/api/messaging/send-message/batch"
                payload = {
                    subject: messageData.subject,
                    content: messageData.content,
                    message_type: messageData.message_type,
                    filters: messageData.filters,
                }
            } else {
                endpoint = `http://127.0.0.1:8000/api/messaging/send-message/user/${messageData.user_id}`
                payload = {
                    subject: messageData.subject,
                    content: messageData.content,
                    message_type: messageData.message_type,
                }
            }

            const response = await fetch(endpoint, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(payload),
            })

            if (!response.ok) {
                const errorData = await response.json()
                throw new Error(errorData.detail || "Lỗi gửi tin nhắn")
            }

            const data = await response.json()
            setResult(data)
            return data
        } catch (err) {
            console.error("Error sending message:", err)
            setError(err.message || "Lỗi không xác định khi gửi tin nhắn")
            throw err
        } finally {
            setIsLoading(false)
        }
    }

    const handleCloseModal = (sendResult) => {
        setIsModalOpen(false)
        if (sendResult) {
            setResult(sendResult)
            // Show success notification
            setTimeout(() => {
                setResult(null)
            }, 5000)
        }
    }

    return (
        <>
            <button
                onClick={() => setIsModalOpen(true)}
                disabled={isLoading}
                className={`flex items-center px-4 py-2 rounded-md text-white ${isBatchMessage ? "bg-purple-600" : "bg-blue-600"
                    } transition-colors`}
            >
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-2" viewBox="0 0 20 20" fill="currentColor">
                    <path d="M2.003 5.884L10 9.882l7.997-3.998A2 2 0 0016 4H4a2 2 0 00-1.997 1.884z" />
                    <path d="M18 8.118l-8 4-8-4V14a2 2 0 002 2h12a2 2 0 002-2V8.118z" />
                </svg>
                {isBatchMessage ? "Gửi tin nhắn hàng loạt" : "Gửi tin nhắn"}
            </button>

            {/* Modal for message form */}
            {isModalOpen && (
                <MessageModal
                    isOpen={isModalOpen}
                    onClose={handleCloseModal}
                    onSend={handleSendMessage}
                    selectedUsers={isBatchMessage ? null : [user]}
                    isBatchMessage={isBatchMessage}
                    filters={filters}
                />
            )}

            {result && (
                <div className="fixed bottom-4 right-4 bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded z-50 shadow-lg">
                    <div className="flex items-center">
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-2" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                                clipRule="evenodd"
                            />
                        </svg>
                        <p>
                            Đã gửi tin nhắn thành công đến {result.sent_count} người dùng
                            {result.failed_count > 0 && `, thất bại: ${result.failed_count}`}
                        </p>
                    </div>
                    <button onClick={() => setResult(null)} className="absolute top-1 right-1 text-green-700">
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
                                clipRule="evenodd"
                            />
                        </svg>
                    </button>
                </div>
            )}

            {error && (
                <div className="fixed bottom-4 right-4 bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded z-50 shadow-lg">
                    <div className="flex items-center">
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-2" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z"
                                clipRule="evenodd"
                            />
                        </svg>
                        <p>{error}</p>
                    </div>
                    <button onClick={() => setError(null)} className="absolute top-1 right-1 text-red-700">
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                            <path
                                fillRule="evenodd"
                                d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
                                clipRule="evenodd"
                            />
                        </svg>
                    </button>
                </div>
            )}
        </>
    )
}

MessageButton.propTypes = {
    user: PropTypes.object,
    filters: PropTypes.object,
    isBatchMessage: PropTypes.bool,
}

export default MessageButton
