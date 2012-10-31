#include "reiserfs.hpp"
#include <stdio.h>
#include <sys/ioctl.h>

Progress::Progress(uint32_t mv)
{
    this->show_percentage = true;
    this->show_raw_values = true;
    this->show_progress_bar = true;
    this->setMaxValue(mv);
    this->prev_ppt = 1001;
    this->prev_value = 0;
}

Progress::~Progress()
{
}

void
Progress::update(uint32_t value)
{
    value = std::min(value, this->max_value);
    this->prev_value = value;
    uint32_t ppt = 1000.0*value/this->max_value;
    if (ppt == this->prev_ppt)  // no visble changes
        return;
    this->prev_ppt = ppt;

    // determine terminal width
    uint32_t width = 79;
    struct winsize ws;
    if (0 == ioctl(1, TIOCGWINSZ, &ws)) {
        width = ws.ws_col - 1;
    }

    printf("\r");
    if (this->show_percentage) {
        int ret = printf("%5.1f%% ", 100.0*value/this->max_value);
        if (ret > 0) width -= ret;
    }
    if (this->show_raw_values) {
        int ret = printf("%d/%d ", value, this->max_value);
        if (ret > 0) width -= ret;
    }
    if (this->show_progress_bar) {
        uint32_t completed = 1.0*(width-2)*value/this->max_value;
        uint32_t rest = (width - 2) - completed;
        printf("[");
        for (uint32_t k = 0; k < completed; k ++) printf("=");
        for (uint32_t k = 0; k < rest; k ++) printf("-");
        printf("]");
        fflush(stdout);
    }
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
Progress::inc()
{
    this->update(this->prev_value + 1);
}
