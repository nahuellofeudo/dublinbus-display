#!/usr/bin/env python3

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
LINE_COUNT = 5
COLOR_LCD : pygame.Color = pygame.Color(244, 203, 96)
COLOR_BACKGROUND = pygame.Color(0, 0, 0)
UPDATE_INTERVAL_SECONDS = 30
TEXT_SIZE = 80  # Size of the font in pixels
STOPS = [
    2410, # College Drive
    1114  # Priory Walk
]

# Offsets of each part within a line
XOFFSET_ROUTE = 12 # 1920x720 -> 24
XOFFSET_DESTINATION = 150 # 1920x720 -> 300
XOFFSEET_TIME_LEFT = 803 # 1920x720 -> 1606
INTER_LINE_SPACE = 0 # 1920x720 -> 0

# Some global variables
window : pygame.Surface = None
font: pygame.font.Font = None
update_queue = queue.Queue(maxsize=10)
dublinbus_client = DublinBusSoapClient(stops=STOPS, update_queue=update_queue, update_interval_seconds=UPDATE_INTERVAL_SECONDS)


def get_line_offset(line: int) -> int:
    global font
    return line * (font.get_height() + INTER_LINE_SPACE)

def write_entry(line: int, 
    route: str = '', destination: str = '', time_left: str = '', 
    time_color: Color = COLOR_LCD, text_color: Color = COLOR_LCD):
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
        write_entry(
            line=line_num,
            route=update.route_id,
            destination=update.destination,
            time_left='Due' if update.isDue() else f'{update.due_in_minutes}min',
            time_color=COLOR_LCD 
        )

def clear_screen() -> None:
    pygame.draw.rect(surface=window, color=COLOR_BACKGROUND, width=0, rect=(0, 0, window.get_width(), window.get_height()))



def main():
    global font
    global window

    """ Main method. Initialise graph """
    pygame.init()
    pygame.display.init()
    pygame.font.init()
    window = pygame.display.set_mode(size=(960, 360), flags=DOUBLEBUF, display=1)
    font = pygame.font.Font(TEXT_FONT, TEXT_SIZE)

    # Paint black
    clear_screen()
    pygame.display.flip()
    dublinbus_client.start()

    # Main event loop
    running = True
    while running:
        pygame.event.wait(timeout=1)
        for e in pygame.event.get():
            if e.type == pygame.QUIT:
                running = False
        #end event handling
        if update_queue.not_empty:
            clear_screen()
            updates = update_queue.get()
            update_screen(updates)
        pygame.display.flip()

    pygame.quit()


if __name__ == "__main__":
    main()