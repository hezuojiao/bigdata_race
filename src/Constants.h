//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_CONSTANTS_H
#define BIGDATA_RACE_CONSTANTS_H


const size_t CUSTOMER = 15000000;
const size_t ORDER    = 150000000;
const size_t LINEITEM = 600037902;

const size_t CUSTOMER_FILE_SIZE = CUSTOMER * sizeof(char);
const size_t ORDER_FILE_SIZE    = ORDER * sizeof(int);
const size_t LINEITEM_FILE_SIZE = LINEITEM * sizeof(int);

//const std::string DATA_PATH = "/data/uname/tpch/";
const std::string DATA_PATH = "./";

const std::string C_MKTSEMENT_PATH = DATA_PATH + "c_mktsegment.cache";

const std::string O_ORDERKEY_PATH  = DATA_PATH + "o_orderkey.cache";
const std::string O_CUSTKEY_PATH   = DATA_PATH + "o_custkey.cache";
const std::string O_ORDERDATE_PATH = DATA_PATH + "o_orderdate.cache";

const std::string L_ORDERKEY_PATH      = DATA_PATH + "l_orderkey.cache";
const std::string L_EXTENDEDPRICE_PATH = DATA_PATH + "l_extendprice.cache";
const std::string L_SHIPDATE_PATH      = DATA_PATH + "l_shipdate.cache";

const int MAX_LENGTH_INSERT_SORT = 12;


// {'BUILDING': 1, 'AUTOMOBILE': 1, 'HOUSEHOLD': 1, 'MACHINERY': 1, 'FURNITURE': 1}

#endif //BIGDATA_RACE_CONSTANTS_H
