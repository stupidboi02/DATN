import React from 'react';
import { useParams, Link } from 'react-router-dom';
import AnalyticsDashboard from './AnalyticsDashboard';

const AnalyticsPage = () => {
    const { userId } = useParams();

    return (
        <div className="min-h-screen bg-gradient-to-b from-gray-50 to-gray-100 p-4 md:p-8">
            <div className="max-w-7xl mx-auto">
                <div className="flex items-center justify-between mb-6 flex-col sm:flex-row gap-4">
                    <h1 className="text-2xl sm:text-3xl font-bold text-gray-800">Thống kê chi tiết: User ID {userId}</h1>
                    <div className="flex gap-4">
                        <Link
                            to="/user-profile"
                            className="flex items-center px-4 py-2 rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors"
                        >
                            <svg
                                xmlns="http://www.w3.org/2000/svg"
                                className="h-5 w-5 mr-2"
                                viewBox="0 0 20 20"
                                fill="currentColor"
                            >
                            </svg>
                            Quay lại
                        </Link>
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
                </div>
                <AnalyticsDashboard userId={userId} />
            </div>
        </div>
    );
};

export default AnalyticsPage;
