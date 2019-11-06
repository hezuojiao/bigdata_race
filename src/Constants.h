//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_CONSTANTS_H
#define BIGDATA_RACE_CONSTANTS_H


const uint32_t CUSTOMER = 75000000;
const uint32_t ORDER    = 750000000;
const uint32_t LINEITEM = 3000028242;

const size_t ORDER_FILE_SIZE    = ORDER * sizeof(uint32_t);
const size_t LINEITEM_FILE_SIZE = LINEITEM * sizeof(uint32_t);

const std::string DATA_PATH = "/data/team_database/";

const std::string C_PATH = DATA_PATH + "customer_";

const std::string O_ORDERKEY_PATH  = DATA_PATH + "o_orderkey.cache";
const std::string O_CUSTKEY_PATH   = DATA_PATH + "o_custkey.cache";
const std::string O_ORDERDATE_PATH = DATA_PATH + "o_orderdate.cache";

const std::string L_ORDERKEY_PATH      = DATA_PATH + "l_orderkey.cache";
const std::string L_EXTENDEDPRICE_PATH = DATA_PATH + "l_extendprice.cache";
const std::string L_SHIPDATE_PATH      = DATA_PATH + "l_shipdate.cache";

const int MAX_LENGTH_INSERT_SORT = 12;

const std::string C_KEYS = "BAHMF";

// {'BUILDING': 1, 'AUTOMOBILE': 1, 'HOUSEHOLD': 1, 'MACHINERY': 1, 'FURNITURE': 1}

#endif //BIGDATA_RACE_CONSTANTS_H
