import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';

const SegmentPage = () => {
    const [formData, setFormData] = useState({
        segmentName: '',
        description: '',
        isActive: true,
        rules: [{ field: '', operator: '', value: '', logic: null }],
        id: null,
    });
    const [segments, setSegments] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [success, setSuccess] = useState('');
    const [isEditing, setIsEditing] = useState(false);
    const [showForm, setShowForm] = useState(false);
    const [deletingId, setDeletingId] = useState(null);

    const fields = [
        { value: 'total_spend', label: 'Tổng tiền chi tiêu' },
        { value: 'total_visits', label: 'Tổng số lần hoạt động' },
        { value: 'total_items_purchased', label: 'Tổng đơn hàng đã mua' },
        { value: 'first_visit_timestamp', label: 'Ngày hoạt động đầu tiên' },
        { value: 'last_visit_timestamp', label: 'Ngày hoạt động gần nhất' },
        { value: 'last_purchase_date', label: 'Ngày mua hàng gần nhất' },
        { value: 'total_support_interactions', label: 'Tổng số lần yêu cầu hỗ trợ' },
        { value: 'avg_satisfaction_score', label: 'Điểm hài lòng trung bình' },
    ];

    const operators = ['=', '>', '<', '>=', '<='];

    const logics = ['AND', 'OR'];

    useEffect(() => {
        fetchSegments();
    }, []);

    const fetchSegments = async () => {
        setLoading(true);
        setError('');
        try {
            const response = await fetch('http://localhost:8888/segments/list');
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            const data = await response.json();
            console.log('Fetched segments:', data); // Debug log
            setSegments(data.filter(segment => segment.segment_id != null)); // Filter out invalid segments
        } catch (err) {
            setError(`Failed to fetch segments: ${err.message}`);
        } finally {
            setLoading(false);
        }
    };

    const handleChange = (e, index) => {
        const { name, value, type, checked } = e.target;
        if (name.startsWith('rule_')) {
            const [, field, idx] = name.split('_');
            const newRules = [...formData.rules];
            newRules[parseInt(idx)][field] = value;
            // Ensure first rule's logic is always null
            if (parseInt(idx) === 0 && field === 'logic') {
                newRules[0].logic = null;
            }
            setFormData((prev) => ({ ...prev, rules: newRules }));
        } else {
            setFormData((prev) => ({
                ...prev,
                [name]: type === 'checkbox' ? checked : value,
            }));
        }
    };

    const addRule = () => {
        setFormData((prev) => ({
            ...prev,
            rules: [...prev.rules, { field: '', operator: '', value: '', logic: 'AND' }],
        }));
    };

    const removeRule = (index) => {
        setFormData((prev) => ({
            ...prev,
            rules: prev.rules.filter((_, i) => i !== index),
        }));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setError('');
        setSuccess('');

        if (!formData.segmentName.trim()) {
            setError('Segment name is required.');
            setLoading(false);
            return;
        }

        for (const rule of formData.rules) {
            if (!rule.field || !rule.operator || !rule.value) {
                setError('All rule fields (field, operator, value) are required.');
                setLoading(false);
                return;
            }

        }

        try {
            const payload = {
                segment_name: formData.segmentName,
                description: formData.description || null,
                is_active: formData.isActive,
                rules: formData.rules.map((rule) => ({
                    field: rule.field,
                    operator: rule.operator,
                    value: rule.value,
                    logic: rule.logic,
                })),
            };

            let response;
            if (isEditing) {
                response = await fetch(`http://localhost:8888/segments/${formData.id}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
            } else {
                response = await fetch('http://localhost:8888/segments', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
            }

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `Failed to ${isEditing ? 'update' : 'create'} segment. Status: ${response.status}`);
            }

            const data = await response.json();
            setSuccess(`Segment "${data.segment_name}" ${isEditing ? 'updated' : 'created'} successfully!`);
            resetForm();
            fetchSegments();
            setShowForm(false);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleEdit = async (segment) => {
        if (!segment.segment_id) {
            setError('Invalid segment ID for editing');
            return;
        }
        try {
            setLoading(true);
            setError('');
            const response = await fetch(`http://localhost:8888/segments/${segment.segment_id}`);
            if (!response.ok) {
                throw new Error(`Failed to fetch segment details. Status: ${response.status}`);
            }
            const data = await response.json();
            setFormData({
                id: data.segment_id,
                segmentName: data.segment_name,
                description: data.description || '',
                isActive: data.is_active,
                rules: data.rules.map((rule, index) => ({
                    field: rule.field,
                    operator: rule.operator,
                    value: rule.value,
                    logic: index === 0 ? null : rule.logic || 'AND',
                })),
            });
            setIsEditing(true);
            setShowForm(true);
        } catch (err) {
            setError(`Failed to load segment: ${err.message}`);
        } finally {
            setLoading(false);
        }
    };

    const handleDelete = async (id) => {
        if (!id || id === 'undefined') {
            setError('Invalid segment ID for deletion');
            return;
        }
        if (!window.confirm('Are you sure you want to delete this segment?')) return;

        setDeletingId(id);
        setError('');
        setSuccess('');

        try {
            const response = await fetch(`http://localhost:8888/segments/${id}`, {
                method: 'DELETE',
            });

            if (!response.ok) {
                if (response.status === 404) {
                    throw new Error('Segment not found');
                }
                const errorData = await response.json();
                throw new Error(errorData.detail || `Failed to delete segment. Status: ${response.status}`);
            }

            setSuccess('Segment deleted successfully!');
            fetchSegments();
        } catch (err) {
            setError(err.message);
        } finally {
            setDeletingId(null);
        }
    };

    const resetForm = () => {
        setFormData({
            id: null,
            segmentName: '',
            description: '',
            isActive: true,
            rules: [{ field: '', operator: '', value: '', logic: null }],
        });
        setIsEditing(false);
        setShowForm(false);
    };

    return (
        <div className="min-h-screen bg-gradient-to-b from-gray-50 to-gray-100 p-4 md:p-8">
            <div className="max-w-7xl mx-auto">
                <header className="bg-white rounded-lg shadow-md p-6 mb-6">
                    <div className="flex justify-between items-center">
                        <h1 className="text-3xl font-bold text-gray-800 mb-4">
                            Manage Segments
                        </h1>
                        <Link
                            to="/"
                            className="px-4 py-2 rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors text-sm"
                        >
                            Trang chủ
                        </Link>
                    </div>
                </header>
                {/* Existing Segments */}
                <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md mb-6">
                    <div className="flex justify-between items-center mb-4">
                        <h2 className="text-lg sm:text-xl font-semibold text-gray-800">Existing Segments</h2>
                        <button
                            onClick={() => {
                                if (showForm) {
                                    setShowForm(false)
                                }
                                else {
                                    resetForm();
                                    setShowForm(true);
                                }
                            }}
                            className="px-4 py-2 rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors text-sm sm:text-base"
                        >
                            Create New Segment
                        </button>
                    </div>
                    {loading && <p className="text-gray-600">Loading segments...</p>}
                    {segments.length === 0 && !loading && <p className="text-gray-600">No segments found.</p>}
                    {segments.length > 0 && (
                        <div className="flex">
                            <table className="w-full text-sm text-gray-700">
                                <thead className="text-xs text-gray-700 uppercase bg-gray-200">
                                    <tr>
                                        <th className="px-4 py-2">Tên Phân Khúc</th>
                                        <th className="px-4 py-2">Mô Tả</th>
                                        <th className="px-4 py-2">Thời Điểm Tạo</th>
                                        <th className="px-4 py-2">Trạng Thái</th>
                                        <th className="px-4 py-2">Hành Động</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {segments.map((segment) => (
                                        <tr key={segment.segment_id} className="border-b">
                                            <td className="px-4 py-2">{segment.segment_name}</td>
                                            <td className="px-4 py-2">{segment.description || '-'}</td>
                                            <td className="px-4 py-2">
                                                {segment.created_at
                                                    ? new Date(segment.created_at).toLocaleString('vi-VN')
                                                    : '-'}
                                            </td>
                                            <td className="px-4 py-2">
                                                {segment.is_active ? 'Active' : 'Inactive'}
                                            </td>
                                            <td className="px-4 py-2 flex gap-2">
                                                <button
                                                    onClick={() => handleEdit(segment)}
                                                    className="text-blue-600 hover:text-blue-500"
                                                    disabled={loading || deletingId === segment.segment_id}
                                                >
                                                    Edit
                                                </button>
                                                <button
                                                    onClick={() => handleDelete(segment.segment_id)}
                                                    className="text-red-600 hover:text-red-700"
                                                    disabled={loading || deletingId === segment.segment_id}
                                                >
                                                    {deletingId === segment.segment_id ? 'Deleting...' : 'Delete'}
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>

                {/* Segment Form (Collapsible) */}
                {showForm && (
                    <div className="bg-white p-4 sm:p-6 rounded-lg shadow-md w-full max-w-lg mx-auto mb-6 animate-fade-in">
                        <h2 className="text-lg sm:text-xl font-semibold text-gray-800 mb-4">
                            {isEditing ? 'Update Segment' : 'Create New Segment'}
                        </h2>
                        <form onSubmit={handleSubmit} className="space-y-4">
                            <div>
                                <label htmlFor="segmentName" className="block text-sm font-medium text-gray-700">
                                    Segment Name *
                                </label>
                                <input
                                    type="text"
                                    id="idSegmentName"
                                    name="segmentName"
                                    value={formData.segmentName}
                                    onChange={handleChange}
                                    className="mt-1 w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                                    placeholder="Enter segment name (e.g., VIP Customers)"
                                    disabled={loading}
                                />
                            </div>
                            <div>
                                <label htmlFor="description" className="block text-sm font-medium text-gray-700">
                                    Description
                                </label>
                                <textarea
                                    id="idDescription"
                                    name="description"
                                    value={formData.description}
                                    onChange={handleChange}
                                    className="mt-1 w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                                    placeholder="Enter segment description (optional)"
                                    rows="3"
                                    disabled={loading}
                                />
                            </div>
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">Rules</label>
                                {formData.rules.map((rule, index) => (
                                    <div key={index} className="border-b border-gray-200 p-3 rounded-md mb-2 space-y-2">
                                        <div className="grid grid-cols-1 sm:grid-cols-4 gap-2">
                                            <select
                                                name={`rule_field_${index}`}
                                                value={rule.field}
                                                onChange={(e) => handleChange(e, index)}
                                                className="px-3 py-2 border border-gray-300 rounded-md sm:text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                                                disabled={loading}
                                            >
                                                <option value="">Select Field</option>
                                                {fields.map((field) => (
                                                    <option key={field.value} value={field.value}>
                                                        {field.label}
                                                    </option>
                                                ))}
                                            </select>
                                            <select
                                                name={`rule_operator_${index}`}
                                                value={rule.operator}
                                                onChange={(e) => handleChange(e, index)}
                                                className="px-3 py-2 border border-gray-300 rounded-md sm:text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                                                disabled={loading}
                                            >
                                                <option value="">Select Operator</option> {operators.map((op) => (
                                                    <option key={op} value={op}>
                                                        {op}
                                                    </option>
                                                ))}
                                            </select>
                                            {rule.field === 'first_visit_timestamp' || rule.field === 'last_visit_timestamp' || rule.field === 'last_purchase_date'
                                                ? (
                                                    <input
                                                        type="date"
                                                        name={`rule_value_${index}`}
                                                        value={rule.value}
                                                        onChange={(e) => handleChange(e, index)}
                                                        className="px-3 py-2 border border-gray-300 rounded-md sm:text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                                                        disabled={loading}
                                                    />
                                                ) : (
                                                    <input
                                                        type="text"
                                                        name={`rule_value_${index}`}
                                                        value={rule.value}
                                                        onChange={(e) => handleChange(e, index)}
                                                        className="px-3 py-2 border border-gray-300 rounded-md sm:text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                                                        placeholder="Value"
                                                        disabled={loading}
                                                    />
                                                )}
                                            {index > 0 && (
                                                <select
                                                    name={`rule_logic_${index}`}
                                                    value={rule.logic}
                                                    onChange={(e) => handleChange(e, index)}
                                                    className="px-3 py-2 border border-gray-300 rounded-md sm:text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                                                    disabled={loading}
                                                >
                                                    <option value="">Select Logic</option>
                                                    {logics.map((logic) => (
                                                        <option key={logic} value={logic}>
                                                            {logic}
                                                        </option>
                                                    ))}
                                                </select>
                                            )}
                                        </div>
                                        {formData.rules.length > 1 && (
                                            <button
                                                type="button"
                                                onClick={() => removeRule(index)}
                                                className="text-red-600 hover:text-red-700 text-sm"
                                                disabled={loading}
                                            >
                                                Remove Rule
                                            </button>
                                        )}
                                    </div>
                                ))}
                                <button
                                    type="button"
                                    onClick={addRule}
                                    className="text-blue-600 hover:text-blue-700 sm:text-sm"
                                    disabled={loading}
                                >
                                    + Add Rule
                                </button>
                            </div>
                            <div className="flex items-center">
                                <input
                                    type="checkbox"
                                    id="isActive"
                                    name="isActive"
                                    checked={formData.isActive}
                                    onChange={handleChange}
                                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                                    disabled={loading}
                                />
                                <label htmlFor="isActive" className="ml-2 text-sm font-medium text-gray-700">
                                    Active
                                </label>
                            </div>
                            {error && (
                                <div className="bg-red-100 text-red-800 p-3 rounded-md text-sm">{error}</div>
                            )}
                            {success && (
                                <div className="bg-green-100 text-green-600 p-3 rounded-md text-sm">{success}</div>
                            )}
                            <div className="flex gap-3">
                                <button
                                    type="submit"
                                    disabled={loading}
                                    className={`flex-1 px-4 py-2 rounded-md text-white sm:text-sm transition-colors ${loading ? 'bg-blue-400 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700'}`}                                >
                                    {loading ? 'Processing...' : isEditing ? 'Update Segment' : 'Create Segment'}
                                </button>
                                <button
                                    type="button"
                                    onClick={resetForm}
                                    className="flex-1 px-4 py-2 rounded-md text-gray-700 bg-gray-200 hover:bg-gray-300 sm:text-sm"
                                >
                                    Cancel
                                </button>
                            </div>
                        </form>
                    </div>
                )}
            </div>
        </div>
    );
};

export default SegmentPage;