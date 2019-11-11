//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_CONSTANTS_H
#define BIGDATA_RACE_CONSTANTS_H


const uint32_t CUSTOMER = 75000000;
const uint32_t ORDER    = 9382016;
const uint32_t LINEITEM = 187529984;


const std::string DATA_PATH = "/data/team_database/";

const std::string O_ORDERKEY_PATH  = DATA_PATH + "o_orderkey-";
const std::string O_ORDERDATE_PATH = DATA_PATH + "o_orderdate-";

const std::string L_ORDERKEY_PATH      = DATA_PATH + "l_orderkey-";
const std::string L_EXTENDEDPRICE_PATH = DATA_PATH + "l_extendprice-";
const std::string L_SHIPDATE_PATH      = DATA_PATH + "l_shipdate-";

const int MAX_LENGTH_INSERT_SORT = 12;
const int MAX_CORE_NUM = 16;
const int MAX_KEYS_NUM = 5;

const char keys[] = "BAHMF";

// {'BUILDING': 1, 'AUTOMOBILE': 1, 'HOUSEHOLD': 1, 'MACHINERY': 1, 'FURNITURE': 1}

#endif //BIGDATA_RACE_CONSTANTS_H
