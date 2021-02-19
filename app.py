from flask import Flask, request, jsonify
import db_impl as data_handler, time

app = Flask(__name__)
data_handler.init()


@app.route('/', methods=['GET'])
def app_help():
    return jsonify("Welcome to Fireblocks HW assignment. This API is used to publish and retrieve messages"
                   "between different users across different topics. Endpoints are:",
                   "/create_topic?topic=your_topic   -   to create a new topic",
                   "/publish?message=your_message&topic=your_topic  -  to publish a message to a topic",
                   "/subscribe?id=your_id&topic=your_topic   -   to subscribe a client to a topic",
                   "/unsubscribe?id=your_id&topic=your_topic   -   to unsubscribe a client from a topic",
                   "/listen?id=your_id   -   to listen for messages for client with that id")


@app.route('/publish', methods=['GET'])
def publish():
    message = request.args.get('message')
    topic = request.args.get('topic')
    try:
        check_args([message, topic])
        return jsonify(data_handler.publish(message, topic))
    except Exception as e:
        return jsonify("Error:", str(e))


@app.route('/create_topic', methods=['GET'])
def create_topic():
    topic = request.args.get('topic')
    try:
        check_args([topic])
        return jsonify(data_handler.create_topic(topic))
    except Exception as e:
        return jsonify("Error:", str(e))


@app.route('/subscribe', methods=['GET'])
def subscribe():
    id = request.args.get('id')
    topic = request.args.get('topic')
    try:
        check_args([id, topic])
        return jsonify(data_handler.subscribe(id, topic))
    except Exception as e:
        return jsonify("Error:", str(e))


@app.route('/unsubscribe', methods=['GET'])
def unsubscribe():
    id = request.args.get('id')
    topic = request.args.get('topic')
    try:
        check_args([id, topic])
        return jsonify(data_handler.unsubscribe(id, topic))
    except Exception as e:
        return jsonify("Error:", str(e))


@app.route('/listen', methods=['GET'])
def listen():
    id = request.args.get('id')
    try:
        check_args([id])
        while True:
            messages = data_handler.listen(id)
            if messages:
                return jsonify(messages)
            else:
                time.sleep(0.5)
    except Exception as e:
        return jsonify("Error:", str(e))


def check_args(args):
    for arg in args:
        if not arg or arg == "":
            raise Exception("wrong input parameters")


if __name__ == "__main__":
    app.run(host='localhost', debug=True, threaded=True)
