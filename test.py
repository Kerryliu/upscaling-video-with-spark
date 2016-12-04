import os
from subprocess import call

command = ["ffmpeg", "-i", "video240Short.m4v", "audio.mp3"]
call(command)

command = ["ffmpeg", "-framerate", "24", "-i", 
        "Frames/ProcessedFrames/output%03d.jpg", "-i", "audio.mp3", "-c:v", 
        "libx264", "-vf", "fps=24", "-pix_fmt", "yuv420p", "out.mp4"]
call(command)


