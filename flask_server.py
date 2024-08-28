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
<header>
    <h1>Welcome to ImageFlow Website Based on Kafka</h1> 
</header>
<head>
<style>
.container {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  grid-auto-rows: minmax(100px, auto);
  gap: 20px;
}
img {
   display: block;
   max-width:100%;
   max-height:100%;
   margin-left: auto;
   margin-right: auto;
}
.img {
   height: 270px;
}
.label {
   height: 30px;
  text-align: center;
}
</style>
</head>
<body>
<form method="post" enctype="multipart/form-data">
  <div>
    <label for="file">Choose file to upload</label>
    <input type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" />
  </div>
  <div>
    <button>Submit</button>
  </div>
</form>
<div class="container">
{% for image in images %}
<div>
<div class="img"><img src="/images/{{ image.filename }}"></img></div>
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
    con = get_db_connection()
    cur = con.cursor()
    json_data = request.json
    object_value = json_data['object']
    cur.execute("UPDATE image SET object = ? WHERE id = ?", (object_value, id))
    con.commit()
    con.close()
    return '{"status": "OK"}'


@app.route('/', methods=['POST'])
def upload_file():
    f = request.files['file']

    file_bytes = np.frombuffer(f.read(), np.uint8)
    image = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)

    if image is None:

        return render_template_string("""
        <script>
        alert("Failed to upload: The file is not a valid image.");
        window.location.href = "/";
        </script>
        """)

    else:
        ext = 'jpeg'
        id = uuid.uuid4().hex
        filename = "{}.{}".format(id, ext)
        filepath = os.path.join(IMAGES_DIR, filename)

  
        cv2.imwrite(filepath, image)  
     
        con = get_db_connection()
        cur = con.cursor()
        cur.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", (id, filename, ""))
        con.commit()

        producer.produce(topic, key=id, value=json.dumps({"id": id, "status": "completed"}))
        producer.flush()
        con.close()

        return render_template_string("""
            <script>
            alert("File uploaded and processed successfully!");
            window.location.href = "/";
            </script>
            """)
   


@app.route('/messages')
def messages():
    data = consume_messages()
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Kafka Messages</title>
        <style>
            .container {
                padding: 20px;
            }
            .section {
                margin-bottom: 20px;
            }
            .section h2 {
                margin-bottom: 10px;
            }
            .message {
                padding: 10px;
                border: 1px solid #ccc;
                margin-bottom: 5px;
            }
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
    app.run(debug=True, port=(int(sys.argv[1]) if len(sys.argv) > 1 else 5000))
