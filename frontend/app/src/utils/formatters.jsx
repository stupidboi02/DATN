export const formatDate = (dateString) => {
    if (!dateString) return "N/A"

    try {
        const date = new Date(dateString)
        if (isNaN(date.getTime())) return "N/A"

        return date.toLocaleDateString("vi-VN", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
        })
    } catch (error) {
        return "N/A"
    }
}

export const formatCurrency = (amount) => {
    if (amount === null || amount === undefined) return "0 ₫"

    try {
        return new Intl.NumberFormat("vi-VN", {
            style: "currency",
            currency: "VND",
        }).format(amount)
    } catch (error) {
        return `${amount} ₫`
    }
}

export const formatNumber = (number) => {
    if (number === null || number === undefined) return "0"

    try {
        return new Intl.NumberFormat("vi-VN").format(number)
    } catch (error) {
        return number.toString()
    }
}
