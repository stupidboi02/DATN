from flask import Flask, jsonify, request
import duckdb

app = Flask(__name__)

# def create_duckdb(year,month):
#     duckdb_file = f"./data/{year}-{month}.duckdb"
#     file_path = f"./data/{year}-{month}.csv"
#     con = duckdb.connect(duckdb_file)
#     con.execute(f"CREATE TABLE IF NOT EXISTS logs AS SELECT * FROM read_csv('{file_path}')")
#     con.execute("CREATE SEQUENCE serial START 1")
#     con.execute("ALTER TABLE logs ADD COLUMN id BIGINT DEFAULT nextval('serial')")
#     con.execute("CHECKPOINT")
#     con.close()

previous_request={
                "year":None,
                "month":None,
                "day":None,
                "total_rows": 0,
                "base_id":0
                }

@app.route("/get-data", methods=['GET'])
def get_data():
    global previous_request
    year = request.args.get('year')
    month = request.args.get('month')
    day = request.args.get('day')
    offset = request.args.get('offset')
    limit = request.args.get('limit')

    # Kiểm tra month hợp lệ
    if not month:
        return jsonify({"error": "Invalid month"}), 400
    
    try:
        offset = int(offset)
        limit = int(limit)
    except ValueError:
        return jsonify({"error": "Invalid offset or limit"}), 400
    
    try:
        con = duckdb.connect(f"./data/{year}-{month}.duckdb",read_only=True)
        
        year,month,day = int(year),int(month),int(day)

        #kiểm tra nếu cùng 1 ngày thì không đếm lại số bản ghi
        if(year != previous_request['year'] 
           or month != previous_request['month'] 
           or day != previous_request['day']):    
            total_rows = con.execute(f"""
                                     SELECT count(*)
                                      FROM logs
                                    WHERE year(event_time) = {year} 
                                     AND month(event_time) = {month} 
                                     AND day(event_time) = {day} 
                                     """).fetchone()[0]  
            base_id = con.execute(f"""
                        SELECT min(id)
                        FROM logs
                        WHERE DATE_TRUNC('day', event_time) = DATE '{year}-{month:02d}-{day:02d}'
                                """).fetchone()[0]
                                                
            previous_request = {
                "year" : year,
                "month" : month,
                "day" : day,
                "total_rows": total_rows,
                "base_id": base_id
            }
        else:
            total_rows = previous_request.get("total_rows")
            base_id = previous_request.get("base_id")
        #sử dụng beetween để loại bỏ các ngày trước đó thay vì phải duyệt lại từ đầu nếu sử dụng "=="
        query = f"""
                SELECT event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
                FROM logs 
                WHERE event_time BETWEEN '{year:04d}-{month:02d}-{day:02d} 00:00:00' AND '{year:04d}-{month:02d}-{day:02d} 23:59:59'
                AND id > {base_id + offset}
                ORDER BY id
                LIMIT {limit}
                """
        # BUGG O DAY
        data = con.execute(query).fetchdf() #execute trả về 1 cursor -> cần fetch để lấy data
        data['event_time'] = data['event_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        

        con.close()

        if offset >= total_rows:
            return jsonify({'state':'complete', 'data': []})

        return jsonify({'state': 'success', 'data': data.to_dict(orient="records")})

    except Exception as e:
        import traceback
        print(f"Error: {e}")
        traceback.print_exc() #in ra chi tiết lỗi
        return jsonify({"state": "error", "data": str(e)}), 500
    
@app.route("/")
def check():
    return '<p>Server is running</p>'

if __name__ == "__main__":
    app.run(debug=True, port=5000)

