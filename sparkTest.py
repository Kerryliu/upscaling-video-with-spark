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

def enhanceImg(image):
    key = image[0]
    image = Image.open(io.BytesIO(image[1]))
    image = numpy.asarray(image, dtype=numpy.uint8)
    image = image.reshape((240,360,3))
    enhancer = enhance.NeuralEnhancer(loader=False)
    out = enhancer.process(image)
    out = out.tobytes()
    return (key, out)

#Video file:
video = 'video240.m4v'

#Remove leftover files from previous attempt
if os.path.exists("Frames"):
    shutil.rmtree("Frames")
os.makedirs("Frames")
os.makedirs("Frames/UnprocessedFrames")

#For simplicity, assume video is 24 fps
command = ["ffmpeg", "-i", video, "-r", "24/1", "-qscale:v", "2",
        "Frames/UnprocessedFrames/output%03d.jpg"]
call(command)

#Convert image to byte type and map them to value
#partitions should be 3~4x the number of cores on the cluster
partitions = 16
images = sc.binaryFiles("Frames/UnprocessedFrames/", partitions)
imageToArray = (lambda rawdata: np.asarray(Image.open(io.StringIO(rawdata))))
#Keys for the images should be their filenames
images.values().map(imageToArray)

enlargedImages = images.map(enhanceImg)
enlargedImages.saveAsTextFile("Frames/ProcessedFrames/")
