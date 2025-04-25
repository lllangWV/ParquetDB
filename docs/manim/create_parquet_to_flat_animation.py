import os

from manim import *

print(type(config))

for key, value in config.items():
    print(key, value)
config.media_dir = os.path.join(os.path.dirname(__file__), "media")

config.save_as_gif = True


class CreateCircle(Scene):
    def construct(self):
        circle = Circle()  # create a circle
        circle.set_fill(PINK, opacity=0.5)  # set the color and transparency
        self.play(Create(circle))  # show the circle on screen


if __name__ == "__main__":
    scene = CreateCircle()
    scene.render()
