#!/usr/bin/env python3
from config import Config
from curses import COLOR_GREEN, COLOR_RED
from datetime import datetime
import os
import gc
from glob import glob
import pygame
from pygame.locals import *
from time import sleep
import queue
from arrival_times import ArrivalTime
from gtfs_client import GTFSClient

# Constants
# The font is JD LCD Rounded by Jecko Development 
# https://fontstruct.com/fontstructions/show/459792/jd_lcd_rounded
TEXT_FONT = 'jd_lcd_rounded.ttf'
LINE_COUNT = 6
COLOR_LCD_AMBER : pygame.Color = pygame.Color(0xf4, 0xcb, 0x60)
COLOR_LCD_GREEN: pygame.Color = pygame.Color(0xb3, 0xff, 0x00)
COLOR_LCD_RED: pygame.Color = pygame.Color(0xff, 0x3a, 0x4a)

COLOR_BACKGROUND = pygame.Color(0, 0, 0)
UPDATE_INTERVAL_SECONDS = 62
TEXT_SIZE = 160  # Size of the font in pixels

# Offsets of each part within a line
XOFFSET_ROUTE = 24
XOFFSET_DESTINATION = 300
XOFFSEET_TIME_LEFT = 1606
INTER_LINE_SPACE = -15 

# Some global variables
window : pygame.Surface = None
font: pygame.font.Font = None
update_queue = queue.Queue(maxsize=10)

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

def write_line(line: int, text: str, text_color: Color = COLOR_LCD_AMBER):
    """ Draws on the screen buffer an arbitrary text. """

    # Step 1: Render the fragments
    text_img = font.render(text, True, text_color)

    # Compose the line
    vertical_offset = get_line_offset(line)
    window.blit(text_img, dest=(XOFFSET_ROUTE, vertical_offset))


def update_screen(config: Config(), updates: list[ArrivalTime]) -> None:
    """ Repaint the screen with the new arrival times """
    updates = updates[0:LINE_COUNT] # take the first X lines
    for line_num, update in enumerate(updates):
        # Find what color we need to use for the ETA
        time_to_walk = update.due_in_minutes - (config.minutes_to_stop(update.stop_id) or 0)
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
            time_left='Due' if update.isDue() else  update.due_in_str(),
            time_color=lcd_color
        )

        # Add the current time to the bottom line
        datetime_text = "Current time: " + datetime.today().strftime("%d/%m/%Y %H:%M")
        write_line(5, datetime_text)

def clear_screen() -> None:
    """ Clear screen """
    pygame.draw.rect(surface=window, color=COLOR_BACKGROUND, width=0, rect=(0, 0, window.get_width(), window.get_height()))


def init_screen() -> pygame.Surface:
    """ Create a Surface to draw on, with the given size, using either X11/Wayland (desktop) or directfb (no desktop) """
    pygame.display.init()
    window = pygame.display.set_mode((0, 0))
    pygame.mouse.set_visible(False)
    return window


def main():
    """ Main function """

    global font
    global window
    global update_queue

    config = Config()

    # Initialise graphics context
    pygame.init()
    window = init_screen()
    pygame.font.init()
    font = pygame.font.Font(TEXT_FONT, TEXT_SIZE)

    # Init screen
    clear_screen()
    write_line(0, "Dublin Bus display")
    write_line(1, "Loading feeds...")
    pygame.display.flip()

    # Create scheduler; load time tables
    scheduler = GTFSClient(feed_url=config.gtfs_feed_url,
                           gtfs_r_url=config.gtfs_api_url,
                           gtfs_r_api_key=config.gtfs_api_key,
                           stop_codes=config.stop_codes, 
                           update_queue=update_queue, 
                           update_interval_seconds=config.update_interval_seconds)

    scheduler.start()

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
            update_screen(config, updates)

            pygame.display.flip()
            gc.collect()
        # Display update ends

        sleep(0.2) 
    pygame.quit()
    exit(0)


if __name__ == "__main__":
    main()