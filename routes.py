from flask import Blueprint, request, jsonify, session
from datetime import datetime

main = Blueprint("main", __name__)

@main.route("/update_date_allsite", methods=["POST"])
def update_date_allsite():
    data = request.get_json()
    selected_date_allsite = data.get("date")

    # ถ้าไม่มีค่า selected_date ให้ใช้วันปัจจุบัน
    if not selected_date_allsite:
        selected_date_allsite = datetime.today().strftime("%Y-%m-%d")

    session["selected_date_allsite"] = selected_date_allsite  # เก็บค่าใน session
    return jsonify({"status": "success", "message": f"วันที่ที่เลือก: {selected_date_allsite}"})

@main.route("/update_date_success", methods=["POST"])
def update_date_success():
    data = request.get_json()
    selected_date_success = data.get("date")

    # ถ้าไม่มีค่า selected_date ให้ใช้วันปัจจุบัน
    if not selected_date_success:
        selected_date_success = datetime.today().strftime("%Y-%m-%d")

    session["selected_date_success"] = selected_date_success  # เก็บค่าใน session
    return jsonify({"status": "success", "message": f"วันที่ที่เลือก: {selected_date_success}"})

@main.route("/update_date_errorData", methods=["POST"])
def update_date_errorData():
    data = request.get_json()
    selected_date_errorData = data.get("date")

    # ถ้าไม่มีค่า selected_date ให้ใช้วันปัจจุบัน
    if not selected_date_errorData:
        selected_date_errorData = datetime.today().strftime("%Y-%m-%d")

    session["selected_date_errorData"] = selected_date_errorData  # เก็บค่าใน session
    return jsonify({"status": "success", "message": f"วันที่ที่เลือก: {selected_date_errorData}"})