
import React, { useState, useEffect } from "react";
import { Line, Pie, Bar } from "react-chartjs-2";
import Flatpickr from "react-flatpickr";
import "flatpickr/dist/flatpickr.min.css";
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend,
} from "chart.js";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend);

const AnalyticsDashboard = ({ userId }) => {
    const [analyticsData, setAnalyticsData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [dateRange, setDateRange] = useState([]);

    const fetchAnalyticsData = async () => {
        setLoading(true);
        setError(null);
        try {
            let url = `http://127.0.0.1:8888/analytics/${userId}`;
            if (dateRange.length === 2) {
                const startDate = dateRange[0].toISOString().split("T")[0];
                const endDate = dateRange[1].toISOString().split("T")[0];
                url += `?start_date=${startDate}&end_date=${endDate}`;
            }

            const response = await fetch(url);
            if (!response.ok) {
                const errorData = await response.json();
                if (response.status === 400) {
                    throw new Error(errorData.detail || "Yêu cầu không hợp lệ.");
                } else if (response.status === 404) {
                    throw new Error(errorData.detail || "Không có dữ liệu thống kê cho người dùng này.");
                } else {
                    throw new Error(`Lỗi API: ${response.status}`);
                }
            }
            const data = await response.json();
            setAnalyticsData(data);
        } catch (err) {
            console.error("Lỗi khi tải dữ liệu thống kê:", err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (userId) fetchAnalyticsData();
    }, [userId, dateRange]);

    if (loading) {
        return (
            <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md text-center">
                <div className="loader mx-auto"></div>
                <p className="mt-4 text-gray-600 text-base">Đang tải dữ liệu thống kê...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-100 text-red-800 p-4 sm:p-6 rounded-lg shadow-md text-center">
                <p className="text-base">{error}</p>
            </div>
        );
    }

    if (!analyticsData || analyticsData.length === 0) {
        return (
            <div className="bg-yellow-100 text-yellow-800 p-4 sm:p-6 rounded-lg shadow-md text-center">
                <p className="text-base">Không có dữ liệu thống kê cho người dùng này.</p>
            </div>
        );
    }

    // Dữ liệu cho biểu đồ
    const dateRangeText = analyticsData.length
        ? `${analyticsData[0].date} đến ${analyticsData[analyticsData.length - 1].date}`
        : "";
    const spendingData = {
        labels: analyticsData.map((item) => item.date) || [],
        datasets: [
            {
                label: "Chi tiêu ($)",
                data: analyticsData.map((item) => item.daily_total_spend || 0) || [],
                fill: false,
                borderColor: "rgb(75, 192, 192)",
                tension: 0.1,
            },
        ],
    };

    const behaviorData = {
        labels: ["Lượt xem", "Thêm vào giỏ", "Mua hàng"],
        datasets: [
            {
                data: [
                    analyticsData.reduce((sum, item) => sum + (item.daily_total_view || 0), 0),
                    analyticsData.reduce((sum, item) => sum + (item.daily_total_add_to_cart || 0), 0),
                    analyticsData.reduce((sum, item) => sum + (item.daily_total_purchase || 0), 0),
                ],
                backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56"],
                borderColor: ["#FF6384", "#36A2EB", "#FFCE56"],
                borderWidth: 1,
            },
        ],
    };

    const purchaseData = {
        labels: analyticsData.map((item) => item.date) || [],
        datasets: [
            {
                label: "Số lượng mua",
                data: analyticsData.map((item) => item.daily_total_purchase || 0) || [],
                backgroundColor: "rgba(54, 162, 235, 0.6)",
            },
        ],
    };

    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: "top",
            },
            title: {
                display: true,
                font: { size: 16 },
            },
        },
        scales: {
            x: {
                title: { display: true, text: "Ngày" },
            },
            y: {
                title: { display: true, text: "Giá trị" },
                beginAtZero: true,
            },
        },
    };

    return (
        <div className="space-y-4 sm:space-y-6 max-w-full mx-auto">
            <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md">
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-2">
                    <h2 className="text-lg sm:text-xl font-semibold text-gray-800">Thống kê khoảng thời gian</h2>
                    <div className="flex flex-col sm:flex-row gap-4 w-full sm:w-auto">
                        <Flatpickr
                            value={dateRange}
                            onChange={(selectedDates) => setDateRange(selectedDates)}
                            options={{
                                mode: "range",
                                dateFormat: "d/m/Y",
                                maxDate: "today",
                                allowInput: true,
                                disableMobile: true,
                            }}
                            className="w-full sm:w-64 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-center text-sm sm:text-base"
                            placeholder="Chọn từ ngày đến ngày..."
                        />
                        <button
                            onClick={fetchAnalyticsData}
                            className="w-full sm:w-auto px-6 py-2 rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors text-sm sm:text-base"
                        >
                            Lấy dữ liệu
                        </button>
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
                <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-800 mb-2 sm:mb-4">
                        Xu hướng chi tiêu {dateRangeText && `(${dateRangeText})`}
                    </h2>
                    <div className="w-full h-64">
                        <Line
                            data={spendingData}
                            options={{ ...chartOptions, plugins: { ...chartOptions.plugins, title: { text: "Chi tiêu theo ngày" } } }}
                        />
                    </div>
                </div>

                <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-800 mb-2 sm:mb-4">
                        Phân bổ hành vi {dateRangeText && `(${dateRangeText})`}
                    </h2>
                    <div className="w-full max-w-xs mx-auto h-64">
                        <Pie
                            data={behaviorData}
                            options={{ ...chartOptions, plugins: { ...chartOptions.plugins, title: { text: "Phân bổ hành vi" } } }}
                        />
                    </div>
                </div>

                <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md lg:col-span-2">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-800 mb-2 sm:mb-4">
                        Lịch sử mua hàng {dateRangeText && `(${dateRangeText})`}
                    </h2>
                    <div className="w-full h-64 sm:h-80">
                        <Bar
                            data={purchaseData}
                            options={{ ...chartOptions, plugins: { ...chartOptions.plugins, title: { text: "Số lượng mua theo ngày" } } }}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default AnalyticsDashboard;