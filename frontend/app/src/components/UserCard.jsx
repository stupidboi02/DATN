"use client"

import PropTypes from "prop-types"
import { formatDate, formatCurrency } from "../utils/formatters"
import MessageButton from "./MessageButton"

function UserCard({ user, isExpanded, onToggleExpand }) {
    // Safe access to nested properties
    const safeAccess = (obj, path, defaultValue = "") => {
        try {
            return (
                path.split(".").reduce((o, key) => (o && o[key] !== undefined && o[key] !== null ? o[key] : null), obj) ||
                defaultValue
            )
        } catch (e) {
            return defaultValue
        }
    }

    // Icons as SVG components
    const ChevronDown = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="text-gray-400"
        >
            <polyline points="6 9 12 15 18 9"></polyline>
        </svg>
    )

    const ChevronUp = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="text-gray-400"
        >
            <polyline points="18 15 12 9 6 15"></polyline>
        </svg>
    )

    const Calendar = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mr-1"
        >
            <rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect>
            <line x1="16" y1="2" x2="16" y2="6"></line>
            <line x1="8" y1="2" x2="8" y2="6"></line>
            <line x1="3" y1="10" x2="21" y2="10"></line>
        </svg>
    )

    const DollarSign = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mr-1"
        >
            <line x1="12" y1="1" x2="12" y2="23"></line>
            <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path>
        </svg>
    )

    const ShoppingBag = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mr-1"
        >
            <path d="M6 2L3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"></path>
            <line x1="3" y1="6" x2="21" y2="6"></line>
            <path d="M16 10a4 4 0 0 1-8 0"></path>
        </svg>
    )

    const AlertTriangle = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="12"
            height="12"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mr-1"
        >
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
            <line x1="12" y1="9" x2="12" y2="13"></line>
            <line x1="12" y1="17" x2="12.01" y2="17"></line>
        </svg>
    )

    const Warning = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="12"
            height="12"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mr-1"
        >
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
            <line x1="12" y1="9" x2="12" y2="15"></line>
            <line x1="12" y1="17" x2="12.01" y2="17"></line>
        </svg>
    )

    const CheckCircle = () => (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="12"
            height="12"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="mr-1"
        >
            <path d="M22 11.08V12a10 10 0 1 1-5.92-9.43"></path>
            <polyline points="22 4 12 14.01 9 11.01"></polyline>
        </svg>
    )

    // Handle potential missing data
    if (!user) {
        return (
            <div className="bg-white rounded-lg shadow-md p-4 text-center">
                <p className="text-gray-500">Dữ liệu người dùng không khả dụng</p>
            </div>
        )
    }

    // Safely get user properties
    const userId = safeAccess(user, "user_id", "N/A")
    const churnRisk = safeAccess(user, "churn_risk", "")
    const segments = safeAccess(user, "segments", "Không có phân khúc")
    const totalVisits = safeAccess(user, "total_visits", 0)
    const totalSpend = safeAccess(user, "total_spend", 0)
    const totalItemsPurchased = safeAccess(user, "total_items_purchased", 0)

    // Safely get timestamps
    const firstVisitTimestamp = safeAccess(user, "first_visit_timestamp", "")
    const lastVisitTimestamp = safeAccess(user, "last_visit_timestamp", "")
    const lastPurchaseDate = safeAccess(user, "last_purchase_date", "")
    const lastActiveDate = safeAccess(user, "last_active_date", "")

    // Safely get purchase history
    const purchaseHistory = safeAccess(user, "purchase_history", [])

    // Safely get preferences
    const categoryPreferences = safeAccess(user, "category_preferences", {})
    const brandPreferences = safeAccess(user, "brand_preferences", {})

    return (
        <div className="bg-white rounded-lg shadow-md overflow-hidden transition-all duration-300">
            <div className="p-4 cursor-pointer hover:bg-gray-50 flex justify-between items-center" onClick={onToggleExpand}>
                <div>
                    <div className="flex items-center gap-2">
                        <h3 className="text-lg font-semibold text-gray-800">User ID: {userId}</h3>
                        <MessageButton user={user} />
                        {(() => {
                            if (churnRisk === "Very High") {
                                return (
                                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-300 text-red-800">
                                        <AlertTriangle />
                                        Rủi ro rất cao
                                    </span>
                                )
                            } else if (churnRisk === "High") {
                                return (
                                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                                        <Warning />
                                        Rủi ro cao
                                    </span>
                                )
                            } else if (churnRisk === "Normal") {
                                return (
                                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                                        <Warning />
                                        Rủi ro trung bình
                                    </span>
                                )
                            } else if (churnRisk === "Low") {
                                return (
                                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                        <CheckCircle />
                                        Rủi ro thấp
                                    </span>
                                )
                            } else {
                                return null
                            }
                        })()}
                    </div>
                    <p className="text-gray-500 text-sm">Phân khúc: {segments}</p>
                </div>

                <div className="flex items-center gap-6">
                    <div className="flex flex-col items-center">
                        <div className="flex items-center text-blue-600">
                            <Calendar />
                            <span className="font-semibold">{totalVisits}</span>
                        </div>
                        <span className="text-xs text-gray-500">Lượt ghé thăm</span>
                    </div>

                    <div className="flex flex-col items-center">
                        <div className="flex items-center text-green-600">
                            <DollarSign />
                            <span className="font-semibold">{formatCurrency(totalSpend)}</span>
                        </div>
                        <span className="text-xs text-gray-500">Chi tiêu</span>
                    </div>

                    <div className="flex flex-col items-center">
                        <div className="flex items-center text-purple-600">
                            <ShoppingBag />
                            <span className="font-semibold">{totalItemsPurchased}</span>
                        </div>
                        <span className="text-xs text-gray-500">Sản phẩm</span>
                    </div>

                    {isExpanded ? <ChevronUp /> : <ChevronDown />}
                </div>
            </div>

            {isExpanded && (
                <div className="border-t border-gray-200 p-4 bg-gray-50">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                            <h4 className="font-medium text-gray-700 mb-2">Thông tin hoạt động</h4>
                            <ul className="space-y-2 text-sm">
                                <li className="flex justify-between">
                                    <span className="text-gray-600">Lần ghé thăm đầu tiên:</span>
                                    <span className="font-medium">{formatDate(firstVisitTimestamp)}</span>
                                </li>
                                <li className="flex justify-between">
                                    <span className="text-gray-600">Lần ghé thăm gần nhất:</span>
                                    <span className="font-medium">{formatDate(lastVisitTimestamp)}</span>
                                </li>
                                <li className="flex justify-between">
                                    <span className="text-gray-600">Lần mua hàng gần nhất:</span>
                                    <span className="font-medium">
                                        {lastPurchaseDate ? formatDate(lastPurchaseDate) : "Chưa mua hàng"}
                                    </span>
                                </li>
                                <li className="flex justify-between">
                                    <span className="text-gray-600">Hoạt động gần nhất:</span>
                                    <span className="font-medium">{formatDate(lastActiveDate)}</span>
                                </li>
                            </ul>
                        </div>

                        <div>
                            <h4 className="font-medium text-gray-700 mb-2">Lịch sử mua hàng</h4>
                            {Array.isArray(purchaseHistory) && purchaseHistory.length > 0 ? (
                                <div
                                    className={`${purchaseHistory.length > 5 ? "purchase-history-scrollable" : "purchase-history-non-scrollable"
                                        }`}
                                >
                                    {purchaseHistory.map((purchase, index) => (
                                        <div key={purchase.order_id || index} className="bg-white p-3 rounded border border-gray-200">
                                            <div className="flex justify-between items-center mb-1">
                                                <span className="text-xs text-gray-500">Đơn hàng #{index + 1}</span>
                                                <span className="text-xs text-gray-500">
                                                    {formatDate(safeAccess(purchase, "order_timestamp", ""))}
                                                </span>
                                            </div>
                                            <div className="flex justify-between">
                                                <span className="text-sm">Tổng tiền:</span>
                                                <span className="text-sm font-medium">
                                                    {formatCurrency(safeAccess(purchase, "total_amount", 0))}
                                                </span>
                                            </div>
                                            <div className="mt-2">
                                                <span className="text-xs text-gray-500">Sản phẩm:</span>
                                                <ul className="mt-1 space-y-1">
                                                    {Array.isArray(safeAccess(purchase, "items", [])) &&
                                                        safeAccess(purchase, "items", []).map((item, itemIndex) => (
                                                            <li key={itemIndex} className="text-xs flex justify-between">
                                                                <span>
                                                                    ID: {safeAccess(item, "product_id", "N/A")} (x{safeAccess(item, "quantity", 1)})
                                                                </span>
                                                                <span>{formatCurrency(safeAccess(item, "price", 0))}</span>
                                                            </li>
                                                        ))}
                                                </ul>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <p className="text-sm text-gray-500">Chưa có lịch sử mua hàng</p>
                            )}
                        </div>
                    </div>

                    <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                            <h4 className="font-medium text-gray-700 mb-2">Danh mục ưa thích</h4>
                            <div className="bg-white p-3 rounded border border-gray-200">
                                {Object.entries(categoryPreferences).length > 0 ? (
                                    Object.entries(categoryPreferences).map(([category, score]) => (
                                        <div key={category} className="flex justify-between items-center mb-1">
                                            <span className="text-sm">{category}</span>
                                            <div className="flex items-center">
                                                <div className="w-24 bg-gray-200 rounded-full h-2 mr-2">
                                                    <div
                                                        className="bg-blue-600 h-2 rounded-full"
                                                        style={{ width: `${Math.min((score || 0) * 20, 100)}%` }}
                                                    ></div>
                                                </div>
                                                <span className="text-xs text-gray-600">
                                                    {typeof score === "number" ? score.toFixed(1) : "0.0"}
                                                </span>
                                            </div>
                                        </div>
                                    ))
                                ) : (
                                    <p className="text-sm text-gray-500">Không có dữ liệu danh mục ưa thích</p>
                                )}
                            </div>
                        </div>

                        <div>
                            <h4 className="font-medium text-gray-700 mb-2">Thương hiệu ưa thích</h4>
                            <div className="bg-white p-3 rounded border border-gray-200">
                                {Object.entries(brandPreferences).length > 0 ? (
                                    Object.entries(brandPreferences).map(([brand, score]) => (
                                        <div key={brand} className="flex justify-between items-center mb-1">
                                            <span className="text-sm">{brand}</span>
                                            <div className="flex items-center">
                                                <div className="w-24 bg-gray-200 rounded-full h-2 mr-2">
                                                    <div
                                                        className="bg-green-600 h-2 rounded-full"
                                                        style={{ width: `${Math.min((score || 0) * 100, 100)}%` }}
                                                    ></div>
                                                </div>
                                                <span className="text-xs text-gray-600">
                                                    {typeof score === "number" ? score.toFixed(1) : "0.0"}
                                                </span>
                                            </div>
                                        </div>
                                    ))
                                ) : (
                                    <p className="text-sm text-gray-500">Không có dữ liệu thương hiệu ưa thích</p>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    )
}

UserCard.propTypes = {
    user: PropTypes.object.isRequired,
    isExpanded: PropTypes.bool.isRequired,
    onToggleExpand: PropTypes.func.isRequired,
}

export default UserCard
