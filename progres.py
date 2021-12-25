import sys


def update_progress(progress):
    bar_length = 100
    block = int(round(bar_length * progress))
    text = "\rPercent: [{0}] {1}% {2}".format("#" * block + "-" * (bar_length - block), round(progress * 100, 2), '')
    sys.stdout.write(text)
    sys.stdout.flush()
