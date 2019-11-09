//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_CONSTANTS_H
#define BIGDATA_RACE_CONSTANTS_H


const uint32_t CUSTOMER = 75000000;
const uint32_t ORDER    = 46875000;
const uint32_t LINEITEMS[] = {
    187468690, 187498207, 187492256, 187503683, 187526924, 187486036,
    187497424, 187516861, 187509857, 187495545, 187497300, 187505088,
    187507207, 187508912, 187496123, 187518129 };

const size_t ORDER_FILE_SIZE = ORDER * sizeof(uint32_t);

const std::string DATA_PATH = "/data/team_database/";

const std::string C_PATH = DATA_PATH + "customer-";

const std::string O_ORDERKEY_PATH  = DATA_PATH + "o_orderkey-";
const std::string O_CUSTKEY_PATH   = DATA_PATH + "o_custkey-";
const std::string O_ORDERDATE_PATH = DATA_PATH + "o_orderdate-";

const std::string L_ORDERKEY_PATH      = DATA_PATH + "l_orderkey-";
const std::string L_EXTENDEDPRICE_PATH = DATA_PATH + "l_extendprice-";
const std::string L_SHIPDATE_PATH      = DATA_PATH + "l_shipdate-";

const int MAX_LENGTH_INSERT_SORT = 12;

const int MAX_CORE_NUM = 16;

const std::string C_KEYS = "BAHMF";

// {'BUILDING': 1, 'AUTOMOBILE': 1, 'HOUSEHOLD': 1, 'MACHINERY': 1, 'FURNITURE': 1}

#endif //BIGDATA_RACE_CONSTANTS_H
