//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_CONSTANTS_H
#define BIGDATA_RACE_CONSTANTS_H

#include <iostream>

const uint32_t CUSTOMER = 75000000;
const uint32_t ORDER = 160000000;
const uint32_t LINEITEM = 3000028242;


const std::string DATA_PATH = "/data/team_database/";

const std::string L_ORDERKEY_PATH      = DATA_PATH + "l_orderkey";
const std::string L_EXTENDEDPRICE_PATH = DATA_PATH + "l_extendprice";
const std::string L_SHIPDATE_PATH      = DATA_PATH + "l_shipdate";

const std::string A_ORDERDATE_PATH        = DATA_PATH + "a_orderdate-";
const std::string A_MINSHIPDATE_PATH      = DATA_PATH + "a_minshipdate-";
const std::string A_MAXSHIPDATE_PATH      = DATA_PATH + "a_maxshipdate-";
const std::string A_EXTENDEDPRICESUM_PATH = DATA_PATH + "a_extendedpricesum-";
const std::string A_ORDERKEY_PATH         = DATA_PATH + "a_orderkey-";
const std::string A_LINEITEMPOSITION_PATH = DATA_PATH + "a_lineitemposition-";

const int MAX_LENGTH_INSERT_SORT = 12;

const int MAX_CORE_NUM = 16;

const int MAX_KEYS_NUM = 5;

const char keys[] = "BAHMF";

// {'BUILDING': 1, 'AUTOMOBILE': 1, 'HOUSEHOLD': 1, 'MACHINERY': 1, 'FURNITURE': 1}

#endif //BIGDATA_RACE_CONSTANTS_H
