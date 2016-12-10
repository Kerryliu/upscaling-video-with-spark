from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("My App")
sc = SparkContext(conf = conf)
from subprocess import call
import os
from glob import glob
import io
import numpy
from PIL import Image
import enhance
import shutil

#Video file:
video = "540.m4v"
#partitions should be 3~4x the number of cores on the cluster
partitions = 8

def enhanceImg(image):
    key = image[0]
    image = Image.open(io.BytesIO(image[1]))
    image = numpy.asarray(image, dtype=numpy.uint8)
    image = image.reshape((540,720,3))
    enhancer = enhance.NeuralEnhancer(loader=False)
    out = enhancer.process(image)
    name = os.path.basename(key)
    out.save("Output/ProcessedFrames/" + name, "JPEG")

#Remove leftover files from previous attempt and recreate dirs
if os.path.exists("Output"):
    shutil.rmtree("Output")
os.makedirs("Output")
os.makedirs("Output/UnprocessedFrames")
os.makedirs("Output/ProcessedFrames")

#extract audio
command = ["ffmpeg", "-i", video, "Output/audio.mp3"]
call(command)

#Extract frames
#For simplicity, assume video is 24 fps
command = ["ffmpeg", "-i", video, "-r", "24/1", "-q:v", "1",
        "Output/UnprocessedFrames/output%03d.jpg"]
call(command)

#Convert image to byte type and map them to value
images = sc.binaryFiles("Output/UnprocessedFrames/", partitions)
imageToArray = (lambda rawdata: np.asarray(Image.open(io.StringIO(rawdata))))
#Keys for the images should be their filenames
images.values().map(imageToArray)
images.foreach(enhanceImg)

#Combine frames into video with sound
command = ["ffmpeg", "-framerate", "24", "-i", 
        "Output/ProcessedFrames/output%03d.jpg", "-i", "Output/audio.mp3", "-c:v", 
        "libx264", "-vf", "fps=24", "-pix_fmt", "yuv420p", "Output/out.mp4"]
call(command)
