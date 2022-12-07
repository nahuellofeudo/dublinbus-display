# dublinbus-sign
Emulates a Dublin Bus electronic sign, showing ETAs for different bus lines

## How to use

1. (optional) Configure the Raspberry Pi to use the non-standard display.
1. Install all dependencies.
1. Download the TTF font into the program's directory.
1. Change main.py, updating STOPS to reflect the stop(s) you want to watch.
1. Run main.py


## 1. Configure the Raspberry Pi

If you plan on using an ultrawide monitor with a similar aspect ratio as the actual Dublin Bus displays (e.g. HSD123KPW2-D10), add the following lines to your /boot/config.txt:

```
framebuffer_width=1920
framebuffer_height=720

hdmi_ignore_edid=0xa5000080
hdmi_group=2
hdmi_mode=87
disable_overscan=1
hdmi_timings=1920 0 88 44 148 720 0 4 5 36 0 0 0 60 0 100980000 1
```

If your display's resolution is not 1920x720, you will also need to change the code to adapt to your specific size.


## 2. Install all dependencies 

* iso8601
* zeep
* libSDL2_ttf-2.0.so.0

```
$ sudo apt install python3-iso8601 python3-zeep libsdl2-ttf-2.0-0
```

* pygame 2

```
$ sudo pip3 install pygame
```


* [TTF Font jd_lcd_rounded.ttf by Jecko Development](https://fontstruct.com/fontstructions/show/459792/jd_lcd_rounded)
  * Download and copy the ttf file in the same folder as the code.
