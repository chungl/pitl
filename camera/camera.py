from picamera2 import Picamera2, MappedArray
from picamera2.encoders import H264Encoder
from picamera2.outputs import CircularOutput, FfmpegOutput
# from overrides import CircularOutput2, FfmpegOutput
from datetime import datetime
import time
import cv2
import subprocess
import os
import atexit

fps = 3
frame_length = int(1_000_000/fps)

tmp_dir="/home/casey/data/tmp/clips/"
clip_dir="/home/casey/data/clips/"

def strftime():
    return datetime.now().strftime('%Y-%m-%dT%H%M%S')

def gen_filename(ext):
    return os.path.join(tmp_dir, f"{strftime()}.{ext}")

def mv(filepath, target_dir):
    basename = os.path.basename(filepath)
    targetpath = os.path.join(target_dir, basename)
    if (targetpath == filepath):
        print('Skipping mv; identity operation {filepath}')
        return
    os.rename(filepath, targetpath)
    return targetpath

colour = (0, 255, 0)
origin = (0, 100)
font = cv2.FONT_HERSHEY_SIMPLEX
scale = 1
thickness = 2

class Camera:
    def __init__(self):
        self.cam = Picamera2()
        self.sensor_modes = self.cam.sensor_modes
        print(self.sensor_modes)


    def start(self):
        print('Starting camera')

        self.config = self.cam.create_video_configuration(
            sensor={"output_size":(2304, 1296)},
            controls={
                "FrameDurationLimits": (frame_length, frame_length)
            }
        )

        self.cam.configure(self.config)
        self.encoder = H264Encoder()        
        self.cam.pre_callback=self.apply_timestamp
        self.cam.start()
    
    def gain(self, gain):
        self.cam.set_controls({'AnalogueGain':float(gain)})

    def set_control(self, control, value):
        controls = {}
        controls[control] = int(value)
        self.cam.set_controls(controls)

    def metadata(self):
        return self.cam.capture_metadata()

    @staticmethod
    def apply_timestamp(request):
        timestamp = time.strftime("%Y-%m-%d %X")
        with MappedArray(request, "main") as m:
            cv2.putText(m.array, timestamp, origin, font, scale, colour, thickness)

    def snapshot(self):
        filename = gen_filename('jpg')
        self.cam.capture_file(filename)
        return filename


    def capture(self, seconds=None):
        filename=gen_filename('h264')
        #print(f'will output to {output.filename}')
        #output.start()

        self.cam.start_recording(self.encoder, filename)
        time.sleep(10)
        self.cam.stop_recording()
        return filename

    def mp4(self, seconds=10):
        output = FfmpegOutput(gen_filename('mp4'))
        self.cam.start_and_record_video(output=output, duration=seconds)

    def circular(self, buffer_s=30, fps=fps):
        try:
            self.cam.stop_encoder(self.encoder)
        except Exception:
            pass
        self.encoder.output = CircularOutput(buffersize=buffer_s*fps)

        self.cam.start_encoder(self.encoder)

    @staticmethod
    def convert_h264_mp4(source_path, outdir=None, fps=fps):
        outfile = source_path.replace('.h264', '.mp4')
        if outdir is not None:
            filename = os.path.basename(outfile)
            outfile = os.path.join(outdir, filename)
        # 
        subprocess.run(['ffmpeg', '-i', source_path, '-vf', f'setpts={30/fps}*PTS', outfile])
        return outfile

    cur_file = None

    def record(self,):
        outfile = gen_filename('h264')
        self.cur_file = outfile
        self.encoder.output.fileoutput = outfile
        self.encoder.output.start()
        return f'{strftime()} Capturing {outfile}'

    def stop(self):
        self.encoder.output.stop()
        h264 = mv(self.cur_file, clip_dir)
        # try:
        #     mp4path = self.convert_h264_mp4(h264, tmp_dir)
        #     print (f'{strftime()} Converted to mp4')
        #     mv(mp4path, clip_dir)
        # except Exception as e:
        #     print (f'{strftime()} Failed to convert mp4 {e}')
        self.cur_file = None
        return f'{strftime()} Done outputting'

    def terminate(self):
        print(f'{strftime()} Stopping camera')
        self.stop()
        return f'{strftime()} Stopping camera'

if __name__ == '__main__':
    cam = Camera()
    cam.start()
    cam.circular(60)
    atexit.register(cam.terminate)
