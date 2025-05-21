# FastAPI MongoDB User Profile API

This project is a FastAPI application that connects to a MongoDB database to retrieve user profile data. The application is structured to facilitate easy development and deployment.

## Project Structure

```
backend
├── src
│   ├── main.py          # Entry point of the FastAPI application
│   ├── db.py            # Database connection logic
│   ├── models.py        # Data models for user profiles
│   └── dependencies.py   # Dependency injection functions
├── requirements.txt      # List of dependencies
└── README.md             # Project documentation
```

## Setup Instructions

1. **Clone the repository**:
   ```
   git clone <repository-url>
   cd backend
   ```

2. **Create a virtual environment**:
   ```
   python -m venv venv
   ```

3. **Activate the virtual environment**:
   - On Windows:
     ```
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```
     source venv/bin/activate
     ```

4. **Install dependencies**:
   ```
   pip install -r requirements.txt
   ```

## Running the Application

To run the FastAPI application, execute the following command:

```
uvicorn src.main:app --reload
```

The application will be available at `http://127.0.0.1:8000`.

## API Usage

### Retrieve User Profiles

- **Endpoint**: `/user_profiles`
- **Method**: `GET`
- **Description**: Fetches user profile data from the MongoDB `user_profile` collection in the `admin` database.

### Example Request

```
GET http://127.0.0.1:8000/user_profiles
```

### Example Response

```json
[
    {
        "id": "1",
        "name": "John Doe",
        "email": "john.doe@example.com"
    },
    {
        "id": "2",
        "name": "Jane Smith",
        "email": "jane.smith@example.com"
    }
]
```

## License

This project is licensed under the MIT License.