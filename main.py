
from flask import Flask
from middleware_module import KafkaMiddleware

app = Flask(__name__)
app.wsgi_app = KafkaMiddleware(app.wsgi_app, topic='your_topic', akto_account_id='your_account_id', limit=10)

@app.route('/')
def index():
    return '{"message": "Hello, World!"}'

if __name__ == '__main__':
    app.run(debug=True)


