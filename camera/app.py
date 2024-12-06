from flask import Flask, request, jsonify
from camera import Camera

app = Flask(__name__)

cam = Camera()
cam.start()
cam.circular(10)


@app.route("/start")
def web_start():
    return f'{cam.record()}<br/><a href="/stop">stop</a>'


@app.route("/stop")
def web_stop():
    return f'{cam.stop()}<br /><a href="/start">start</a>'


@app.route("/gain/<gain>")
def web_gain(gain):
    cam.gain(gain)
    return jsonify(cam.metadata())


@app.route("/metadata/<control>/<value>")
def web_controls(control, value):
    cam.set_control(control, value)
    return jsonify([cam.metadata()])


@app.route("/metadata")
def web_metadata():
    return jsonify([cam.metadata()])
