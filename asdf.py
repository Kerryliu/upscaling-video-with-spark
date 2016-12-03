import subprocess as sp
from PIL import Image
import numpy
import enhance
import matplotlib.pyplot as plt
import numpy as np
import scipy.ndimage, scipy.misc, PIL.Image
command = ['ffmpeg',
            '-i', 'video480.wmv',
            '-ss', '00:1:00',
            '-f', 'image2pipe',
            '-pix_fmt', 'rgb24',
            '-vcodec','rawvideo', '-']
pipe = sp.Popen(command, stdout = sp.PIPE, bufsize=10**8)

# read 420*360*3 bytes (= 1 frame)
raw_image = pipe.stdout.read(720*480*3)
# transform the byte read into a numpy array
image =  numpy.fromstring(raw_image, dtype='uint8')
image = image.reshape((480,720,3))
# throw away the data in the pipe's buffer.
pipe.stdout.flush()
enhancer = enhance.NeuralEnhancer(loader=False)
out = enhancer.process(image)
out.save('abc.png')
