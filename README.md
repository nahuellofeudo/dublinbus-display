# dublinbus-display
Emulates a Dublin Bus electronic sign, showing ETAs for different bus lines

## How to use it
1. Clone the repository into your /home/pi directory.
1. (optional) Configure the Raspberry Pi to use the non-standard display.
1. Install all dependencies.
1. Download the TTF font into the program's directory.
1. Change main.py, updating STOPS to reflect the stop(s) you want to watch.
1. Run main.py



## 1. Configure the Raspberry Pi (pi-specific)

### 1.1 - Disable boot messages

Open ```/boot/cmdline.txt``` and add the parameter ```quiet``` at the end of the line.

### 1.2 - Configure the display aspect ratio (only for ultrawide monitors)

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
* python3-numpy
* python3-pandas
* python3-fiona
* python3-pyproj (to build gtfs_kit2)
* libspatialindex-c6
* yaml

```shell
$ sudo apt install python3-iso8601 python3-zeep libsdl2-ttf-2.0-0 python3-numpy python3-pandas python3-fiona python3-pyproj libspatialindex-c6 python3-yaml
```

* pygame 2
* GTFS-Kit

```shell
$ sudo apt install python3-pip
$ sudo pip3 install pygame gtfs_kit schedule --break-system-packages
```


* [TTF Font jd_lcd_rounded.ttf by Jecko Development](https://fontstruct.com/fontstructions/show/459792/jd_lcd_rounded)
  * Download and copy the ttf file in the same folder as the code.


## 3. Set up the services

### 3.1 - Disable login on tty1:

```
$ sudo systemctl disable getty@tty1
$ sudo systemctl stop getty@tty1
```

### 3.2 - Create a service file to auto-start the display program

First, create a link from the provided systemd service into the systemd directory

```
$ sudo ln -s /home/pi/dublinbus-display/systemd/dublinbus-display.service /etc/systemd/system/
```

Enable and start the service

```
$ sudo systemctl daemon-reload
$ sudo systemctl enable dublinbus-display
$ sudo systemctl start dublinbus-display
```

Restart the system

```
$ sudo reboot
```

