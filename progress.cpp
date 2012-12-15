/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#include "reiserfs.hpp"
#include <stdio.h>
#include <sys/ioctl.h>

Progress::Progress(uint32_t mv)
{
    this->show_percentage = true;
    this->show_raw_values = true;
    this->show_progress_bar = true;
    this->show_name = false;
    this->setMaxValue(mv);
    this->prev_ppt = 1001;
    this->prev_value = 0;
    this->unknown_mode = false;
    this->start_time = time(NULL);
    this->unknown_interval = 1;
}

Progress::~Progress()
{
}

void
Progress::update(uint32_t value)
{
    if (this->unknown_mode) {
        this->prev_value = value;
        this->displayUnknown(value);
    } else {
        value = std::min(value, this->max_value);
        this->prev_value = value;
        uint32_t ppt = 1000.0*value/this->max_value;
        if (ppt == this->prev_ppt)  // no visble changes
            return;
        this->prev_ppt = ppt;
        this->displayKnown(value);
    }
}

uint32_t
Progress::getWidth()
{
    uint32_t width = 79;
    struct winsize ws;
    if (0 == ioctl(1, TIOCGWINSZ, &ws)) width = ws.ws_col - 1;
    return width;
}

void
Progress::displayUnknown(uint32_t value)
{
    if (0 == (value % this->unknown_interval)) {
        int32_t width = this->getWidth();
        time_t now = time(NULL);
        int32_t delta = now - this->start_time;

        printf("\r");
        if (this->show_name) {
            int ret = printf("%s ", this->name.c_str());
            if (ret > 0) width -= ret;
        }
        if (this->show_raw_values) {
            int ret = printf("%d/? ", value);
            if (ret > 0) width -= ret;
        }
        if (this->show_progress_bar) {
            const int32_t pos = delta % (width - 2 - 3);
            const int32_t fill = (width - 2 - 3 - pos);

            printf("[");
            for (int32_t k = 0; k < pos; k ++) printf("-");
            printf("<=>");
            for (int32_t k = 0; k < fill; k ++) printf("-");
            printf("]");
        }
        fflush(stdout);
    }
}

void
Progress::displayKnown(uint32_t value)
{
    int32_t width = this->getWidth();

    printf("\r");
    if (this->show_name) {
        int ret = printf("%s ", this->name.c_str());
        if (ret > 0) width -= ret;
    }
    if (this->show_percentage) {
        int ret = printf("%5.1f%% ", 100.0*value/this->max_value);
        if (ret > 0) width -= ret;
    }
    if (this->show_raw_values) {
        int ret = printf("%d/%d ", value, this->max_value);
        if (ret > 0) width -= ret;
    }
    if (this->show_progress_bar) {
        int32_t completed = 1.0*(width-2)*value/this->max_value;
        int32_t rest = (width - 2) - completed;
        printf("[");
        for (int32_t k = 0; k < completed; k ++) printf("=");
        for (int32_t k = 0; k < rest; k ++) printf("-");
        printf("]");
    }
    fflush(stdout);
}

void
Progress::show100()
{
    this->update(this->max_value);
    printf("\n");
    fflush(stdout);
}

void
Progress::abort()
{
    printf("\n");   fflush(stdout);
}

void
Progress::inc(uint32_t delta)
{
    this->update(this->prev_value + delta);
}
