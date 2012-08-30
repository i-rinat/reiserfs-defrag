#include "reiserfs.hpp"

FsBitmap::FsBitmap(FsJournal *journal_)
{
    this->journal = journal_;

}

FsBitmap::~FsBitmap()
{
}
