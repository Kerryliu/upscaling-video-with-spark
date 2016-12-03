from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
from subprocess import call
import os
from glob import glob

#Video file:
video = 'video480.wmv'

#First remove leftover files:
folders = glob("./Frames/*")
for i in folders:
    files = glob(i + "/*")
    for j in files:
        os.remove(j)

#For simplicity, assume video is 24 fps
command = ["ffmpeg", "-i", video, "-r",
        "24/1", "Frames/UnprocessedFrames/output%03d.jpg"]
call(command)
