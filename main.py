#!/usr/bin/env python3
from curses import COLOR_GREEN, COLOR_RED
import os
from glob import glob
import pygame
from pygame.locals import *
from time import sleep
from dublinbus_soap_client import DublinBusSoapClient
import queue
from arrival_times import ArrivalTime

# Constants
# The font is JD LCD Rounded by Jecko Development 
# https://fontstruct.com/fontstructions/show/459792/jd_lcd_rounded
TEXT_FONT = 'jd_lcd_rounded.ttf'
LINE_COUNT = 6
COLOR_LCD_AMBER : pygame.Color = pygame.Color(0xf4, 0xcb, 0x60)
COLOR_LCD_GREEN: pygame.Color = pygame.Color(0xb3, 0xff, 0x00)
COLOR_LCD_RED: pygame.Color = pygame.Color(0xff, 0x3a, 0x4a)

COLOR_BACKGROUND = pygame.Color(0, 0, 0)
UPDATE_INTERVAL_SECONDS = 30
TEXT_SIZE = 160  # Size of the font in pixels
STOPS = [
    2410, # College Drive
    1114  # Priory Walk
]

# Define how long it takes to walk to a particular stop
MINUTES_TO_ROUTE = {
    '15A': 15,
    '54A': 9
}

# Offsets of each part within a line
XOFFSET_ROUTE = 24
XOFFSET_DESTINATION = 300
XOFFSEET_TIME_LEFT = 1606
INTER_LINE_SPACE = -20 # 1920x720 -> 0

# Some global variables
window : pygame.Surface = None
font: pygame.font.Font = None
update_queue = queue.Queue(maxsize=10)
dublinbus_client = DublinBusSoapClient(stops=STOPS, update_queue=update_queue, update_interval_seconds=UPDATE_INTERVAL_SECONDS)


def get_line_offset(line: int) -> int:
    """ Calculate the Y offset within the display for a given text line """
    global font
    return line * (font.get_height() + INTER_LINE_SPACE)


def write_entry(line: int, 
    route: str = '', destination: str = '', time_left: str = '', 
    time_color: Color = COLOR_LCD_AMBER, text_color: Color = COLOR_LCD_AMBER):
    """ Draws on the screen buffer an entry corresponding to an arrival time. """

    # Step 1: Render the fragments
    route_img = font.render(route[0:4], True, text_color)
    destination_img = font.render(destination[0:21], True, text_color)
    time_left_img = font.render(time_left[0:5], True, time_color)

    # Compose the line
    vertical_offset = get_line_offset(line)
    window.blit(route_img, dest=(XOFFSET_ROUTE, vertical_offset))
    window.blit(destination_img, dest=(XOFFSET_DESTINATION, vertical_offset))
    window.blit(time_left_img, dest=(XOFFSEET_TIME_LEFT, vertical_offset))


def update_screen(updates: list[ArrivalTime]) -> None:
    """ Repaint the screen with the new arrival times """
    updates = updates[0:LINE_COUNT] # take the first X lines
    for line_num, update in enumerate(updates):
        # Find what color we need to use for the ETA
        time_to_walk = update.due_in_minutes - (MINUTES_TO_ROUTE.get(update.route_id) or 0)
        lcd_color = None
        if time_to_walk > 5:
            lcd_color = COLOR_LCD_GREEN
        elif time_to_walk > 1:
            lcd_color = COLOR_LCD_AMBER
        else:
            lcd_color = COLOR_LCD_RED

        # Draw the line
        write_entry(
            line=line_num,
            route=update.route_id,
            destination=update.destination,
            time_left='Due' if update.isDue() else f'{update.due_in_minutes}min',
            time_color=lcd_color
        )


def clear_screen() -> None:
    """ Clear screen """
    pygame.draw.rect(surface=window, color=COLOR_BACKGROUND, width=0, rect=(0, 0, window.get_width(), window.get_height()))


def init_screen(width: int, height: int) -> pygame.Surface:
    """ Create a Surface to draw on, with the given size, using either X11/Wayland (desktop) or directfb (no desktop) """
    drivers = ['x11', 'directfb']
    for driver in drivers:
        print(f'Trying driver {driver}')
        # Make sure that SDL_VIDEODRIVER is set
        os.putenv('SDL_VIDEODRIVER', driver)
        try:
            pygame.display.init()
            window = pygame.display.set_mode(size=(width, height), flags=DOUBLEBUF)
        except pygame.error:
            continue
        return window
    raise Exception('No suitable video driver found!')


def main():
    """ Main function """

    global font
    global window

    """ Main method. Initialise graphics context """
    pygame.init()
    window = init_screen(1920, 720)
    pygame.font.init()
    font = pygame.font.Font(TEXT_FONT, TEXT_SIZE)

    # Paint black
    clear_screen()
    pygame.display.flip()
    dublinbus_client.start()

    # Main event loop
    running = True
    while running:
        # Pygame event handling begins
        if pygame.event.peek():
            for e in pygame.event.get():
                if e.type == pygame.QUIT:
                    running = False
                elif e.type == pygame.KEYDOWN:
                    if e.key == pygame.K_ESCAPE:
                        running = False
            pygame.display.flip()
        # Pygame event handling ends

        # Display update begins
        if update_queue.qsize() > 0:
            clear_screen()
            updates = update_queue.get()
            update_screen(updates)

            pygame.display.flip()
        # Display update ends

        sleep(0.2) 
    pygame.quit()
    exit(0)


if __name__ == "__main__":
    main()