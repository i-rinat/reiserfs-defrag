#include "reiserfs.hpp"
#include <iostream>

ReiserFs::ReiserFs()
{

}

ReiserFs::~ReiserFs()
{

}

int
ReiserFs::open(std::string name)
{
    std::cout << "open " << name << std::endl;
    this->fname = name;
}

int
ReiserFs::close()
{
    std::cout << "close " << this->fname << std::endl;
}

int
ReiserFs::moveBlock(uint32_t from, uint32_t to)
{
    std::cout << "from: " << from << ", to: " << to << std::endl;
}

int
ReiserFs::beginTransaction()
{
    std::cout << "beginTransaction" << std::endl;
}

int
ReiserFs::commitTransaction()
{
    std::cout << "commitTransaction" << std::endl;
}
