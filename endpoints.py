from flask import Flask, request, jsonify
from pymongo import MongoClient
from concurrent.futures import ProcessPoolExecutor
import time
from threading import Lock

app = Flask(__name__)

# MongoDB connection 
mongo_uri = 'mongodb://'
client = MongoClient(mongo_uri, minPoolSize=10, maxPoolSize=50)

def get_db(db_name):
    return client[db_name]

# Custom HTTP error handling                    
@app.errorhandler(Exception)
def handle_exception(e):
    response = jsonify({
        "code": e.code if hasattr(e, 'code') else 500, 
        "name": e.name if hasattr(e, 'name') else 'Internal Server Error', 
        "description": e.description if hasattr(e, 'description') else str(e),
    })
    response.status_code = e.code if hasattr(e, 'code') else 500
    return response

# Global timer and lock
start_time = None
lock = Lock()

# Middleware to measure total request time
@app.before_request
def start_timer():
    global start_time
    start_time = time.time()

@app.after_request
def log_total_request_time(response):
    global start_time
    with lock:
        if request.endpoint == 'static':
            return response  # Exclude static files from timing
        total_time = time.time() - start_time
        print(f"All requests completed. Total time: {total_time:.4f} seconds")
    return response
  
# Function to perform document count based on filters
def count_documents(db_name, collection_name, filters):
    db = get_db(db_name)
    collection = db[collection_name]
    pipeline = [
        {"$match": filters},
        {"$count": "total"}
    ]
    result = list(collection.aggregate(pipeline))
    total = result[0]['total'] if result else 0
    return total

# Function to get paired and non-paired documents and calculate percentages
def count_paired_and_non_paired_with_percentages(db_name, collection_name, filters):
    db = get_db(db_name)
    collection = db[collection_name]
    pipeline = [
        {"$match": filters},
        {"$group": {
            "_id": "$paired",
            "count": {"$sum": 1}
        }}
    ]
    result = list(collection.aggregate(pipeline))

    total_paired = 0
    total_non_paired = 0
    for res in result:
        if res['_id']:
            total_paired += res['count']
        else:
            total_non_paired += res['count']

    total_all = total_paired + total_non_paired
    percentage_paired = (total_paired / total_all * 100) if total_all > 0 else 0
    percentage_non_paired = (total_non_paired / total_all * 100) if total_all > 0 else 0

    return total_paired, total_non_paired, percentage_paired, percentage_non_paired

# run all queries in parallel using ProcessPoolExecutor
def parallel_queries(filters):
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(count_documents, 't2_compostage_db', 'copies2', filters),
            executor.submit(count_paired_and_non_paired_with_percentages, 't2_compostage_db', 'copies2', filters),
            executor.submit(count_documents, 't2_compostage_db', 'copies2', {**filters, 'status': 'absent'}),
            executor.submit(count_documents, 't2_compostage_db', 'candidats2', {})
        ]
        results = [future.result() for future in futures if future.result() is not None]
    return results

# Route to execute all queries in parallel and calculate total time
@app.route('/parallel_queries', methods=['GET'])
def execute_parallel_queries():
    filters = {}
    c_Academy = request.args.get('c_Academy')
    type_Exam = request.args.get('type_Exam')
    codeD_P = request.args.get('codeD_P')
    C_F = request.args.get('C_F')
    typeCandidat = request.args.get('typeCandidat')
    c_Center = request.args.get('c_Center')
    code_S = request.args.get('code_S')
    N_exam = request.args.get('N_exam')

    if c_Academy:
        filters['c_Academy'] = c_Academy
    if type_Exam:
        filters['type_Exam'] = int(type_Exam)
    if codeD_P:
        filters['codeD_P'] = codeD_P
    if C_F:
        filters['C_F'] = C_F
    if typeCandidat:
        filters['typeCandidat'] = typeCandidat
    if c_Center:
        filters['c_Center'] = c_Center
    if code_S:
        filters['code_S'] = int(code_S)
    if N_exam:
        filters['N_exam'] = int(N_exam)

    start_time = time.time()
    results = parallel_queries(filters)
    total_time = time.time() - start_time

    total_paired, total_non_paired, percentage_paired, percentage_non_paired = results[1]
    total_absent = results[2]
    total_candidates = results[3]

    return jsonify({
        "total_paired": total_paired,
        "total_absent": total_absent,
        "percentage_paired": percentage_paired,
        "percentage_non_paired": percentage_non_paired,
        "total_candidates": total_candidates,
        "total_time": total_time
   
    }), 200

if __name__ == '__main__':
    app.run(debug=True)
