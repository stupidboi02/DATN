
import React from 'react';
import { Link } from 'react-router-dom';

const HomePage = () => {
    return (
        <div className="min-h-screen bg-gradient-to-b from-gray-50 to-gray-100 p-4 md:p-8">
            <div className="max-w-7xl mx-auto">
                {/* Header */}
                <header className="bg-white rounded-lg shadow-md p-6 mb-6">
                    <h1 className="text-3xl font-bold text-gray-800 mb-4">Chào mừng đến với Hệ thống Quản lý Người dùng</h1>
                    <p className="text-gray-600">Chọn một chức năng từ menu bên dưới để bắt đầu.</p>
                </header>

                {/* Navigation Menu */}
                <nav className="bg-white rounded-lg shadow-md p-4">
                    <ul className="flex flex-col sm:flex-row gap-4">
                        <div>
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
                                    <path d="M10 9a3 3 0 100-6 3 3 0 000 6zm-7 9a7 7 0 1114 0H3z" />
                                </svg>
                                User Profile
                            </Link>
                        </div>
                        <div>
                            <Link
                                to="/dashboard"
                                className="flex items-center px-4 py-2 rounded-md text-white bg-green-600 hover:bg-green-700 transition-colors"
                            >
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    className="h-5 w-5 mr-2"
                                    viewBox="0 0 20 20"
                                    fill="currentColor"
                                >
                                    <path d="M3 4a1 1 0 011-1h12a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1V4zM3 10a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H4a1 1 0 01-1-1v-6zM14 10a1 1 0 00-1 1v6a1 1 0 001 1h2a1 1 0 001-1v-6a1 1 0 00-1-1h-2z" />
                                </svg>
                                Dashboard
                            </Link>
                        </div>
                        <div>
                            <Link
                                to="/get-data"
                                className="flex items-center px-4 py-2 rounded-md text-white bg-purple-600 hover:bg-purple-700 transition-colors"
                            >
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    className="h-5 w-5 mr-2"
                                    viewBox="0 0 20 20"
                                    fill="currentColor"
                                >
                                    <path
                                        fillRule="evenodd"
                                        d="M10 18a8 8 0 100-16 8 8 0 000 16zM7 9a1 1 0 000 2h6a1 1 0 100-2H7z"
                                        clipRule="evenodd"
                                    />
                                </svg>
                                Get Data
                            </Link>
                        </div>
                    </ul>
                </nav>
            </div>
        </div>
    );
};

export default HomePage;