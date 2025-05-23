from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/query", methods=["POST"])
def query():
    sql = request.json.get("sql")
    return jsonify({"response": f"Received query: {sql}"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005)
