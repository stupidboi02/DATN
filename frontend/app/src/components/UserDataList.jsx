"use client"

import { useState } from "react"
import PropTypes from "prop-types"
import UserCard from "./UserCard"

function UserDataList({ userData }) {
    const [expandedUser, setExpandedUser] = useState(null)

    const toggleExpand = (userId) => {
        if (expandedUser === userId) {
            setExpandedUser(null)
        } else {
            setExpandedUser(userId)
        }
    }

    if (userData.length === 0) {
        return (
            <div className="bg-white rounded-lg shadow-md p-8 text-center">
                <p className="text-gray-500 text-lg">Không tìm thấy dữ liệu người dùng phù hợp với bộ lọc.</p>
            </div>
        )
    }

    return (
        <div className="grid grid-cols-1 gap-4">
            {userData.map((user) => (
                <UserCard
                    key={user._id}
                    user={user}
                    isExpanded={expandedUser === user._id}
                    onToggleExpand={() => toggleExpand(user._id)}
                />
            ))}
        </div>
    )
}

UserDataList.propTypes = {
    userData: PropTypes.array.isRequired,
}

export default UserDataList
