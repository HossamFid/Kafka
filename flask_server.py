import os,sqlite3,sys,uuid, cv2 
from confluent_kafka import Producer
import json
import numpy as np
from flask import (Flask, redirect, render_template_string, request,send_from_directory, flash)
from kafka_consumer_msg import consume_messages


me = 'HossamFid-01'
topic = me 
ERROR_TOPIC = me + 'error-topic'
print(ERROR_TOPIC)

conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094', \
    'client.id': me
}
producer = Producer(conf)

app = Flask(__name__)

MAIN_DB = "main.db"
def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn

con = get_db_connection()
con.execute("CREATE TABLE IF NOT EXISTS image(id, filename, object)")

IMAGES_DIR = "images"
if not os.path.exists(IMAGES_DIR):
    os.mkdir(IMAGES_DIR)
     

@app.route('/', methods=['GET'])
def index():
    con = get_db_connection()
    cur = con.cursor()
    res = cur.execute("SELECT * FROM image")
    images = res.fetchall()
    con.close()
    return render_template_string("""
<!DOCTYPE html>
<html>
<meta http-equiv="refresh" content="10">
<head>
    <title>ImageFlow - Kafka Powered</title>
    <style>
        .container { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 20px; }
        .img { height: 270px; }
        .label { text-align: center; height: 30px; }
        img { display: block; max-width:100%; max-height:100%; margin: auto; }
    </style>
</head>
<body>
    <h1>Welcome to ImageFlow Website</h1> 
    <form method="post" enctype="multipart/form-data">
        <label for="file">Choose file to upload</label>
        <input type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" required/>
        <button type="submit">Submit</button>
    </form>
    <div class="container">
    {% for image in images %}
        <div>
            <div class="img"><img src="/images/{{ image.filename }}" alt="Image"></div>
            <div class="label">{{ image.object | default('un-defined', true) }}</div>
        </div>
    {% endfor %}
    </div>
</body>
</html>
    """, images=images)

@app.route('/images/<path:path>', methods=['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)

@app.route('/object/<id>', methods=['PUT'])
def set_object(id):
    """Update the object label of an image by ID."""
    con = get_db_connection()
    json_data = request.json
    object_value = json_data.get('object', '')
    con.execute("UPDATE image SET object = ? WHERE id = ?", (object_value, id))
    con.commit()
    con.close()
    return '{"status": "OK"}'


@app.route('/', methods=['POST'])
def upload_file():
    """Handle file upload, processing, and Kafka message production."""
    f = request.files['file']
    
    # Decode the image
    file_bytes = np.frombuffer(f.read(), np.uint8)
    image = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)

    if image is None:
        # If the file is not a valid image
        return render_template_string("""
        <script>
        alert("Failed to upload: The file is not a valid image.");
        window.location.href = "/";
        </script>
        """)
    else:
        # Save the valid image to disk and record in the database
        filename = f"{uuid.uuid4().hex}.jpeg"
        filepath = os.path.join(IMAGES_DIR, filename)
        cv2.imwrite(filepath, image)

        # Insert into the database
        con = get_db_connection()
        con.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", 
                    (uuid.uuid4().hex, filename, ""))
        con.commit()

        # Produce Kafka message
        producer.produce(TOPIC, key=filename, value=json.dumps({"filename": filename, "status": "completed"}))
        producer.flush()

        # Confirmation
        return render_template_string("""
            <script>
            alert("File uploaded and processed successfully!");
            window.location.href = "/";
            </script>
        """)

@app.route('/messages', methods=['GET'])
def messages():
    """Display Kafka error and completed messages."""
    data = consume_messages()
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Kafka Messages</title>
        <style>
            .container { padding: 20px; }
            .section { margin-bottom: 20px; }
            .message { padding: 10px; border: 1px solid #ccc; margin-bottom: 5px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="section">
                <h2>Error Messages</h2>
                {% for message in data.errors %}
                    <div class="message">{{ message }}</div>
                {% endfor %}
            </div>
            <div class="section">
                <h2>Completed Messages</h2>
                {% for message in data.completed %}
                    <div class="message">{{ message }}</div>
                {% endfor %}
            </div>
        </div>
    </body>
    </html>
    """, data=data)

if __name__ == '__main__':
    app.run(debug=True, port=(int(sys.argv[1]) if len(sys.argv) > 1 else 500