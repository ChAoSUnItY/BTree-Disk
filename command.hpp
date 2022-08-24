//
// Created by chaos on 2022/8/17.
//

#ifndef SHNODATABASE_COMMAND_HPP
#define SHNODATABASE_COMMAND_HPP

#include <string>
#include <regex>

extern const std::regex regexes[9];

void parseCommand(std::string input);

#endif //SHNODATABASE_COMMAND_HPP
