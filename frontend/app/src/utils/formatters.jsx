export function formatDate(dateString) {
    try {
        const date = new Date(dateString)
        if (isNaN(date.getTime())) {
            return "Không tồn tại"
        }
        return new Intl.DateTimeFormat("vi-VN", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
        }).format(date)
    } catch (error) {
        console.error("Error formatting date:", error)
        return "Lỗi định dạng ngày"
    }
}

export function formatCurrency(amount) {
    try {
        if (typeof amount !== 'number') {
            return '0 ₫'
        }
        return new Intl.NumberFormat("vi-VN", {
            style: "currency",
            currency: "VND",
            minimumFractionDigits: 0,
        }).format(amount)
    } catch (error) {
        console.error("Error formatting currency:", error)
        return "Lỗi định dạng tiền tệ"
    }
}
