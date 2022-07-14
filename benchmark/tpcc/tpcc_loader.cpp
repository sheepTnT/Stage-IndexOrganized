//
// Created by zhangqian on 2022-03-1.
//


//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_loader.cpp
//
// Identification: src/main/tpcc/tpcc_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cstring>
#include <random>

#include "tpcc_record.h"
#include "tpcc_loader.h"
#include "tpcc_configuration.h"


#ifdef __APPLE__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-const-variable"
#endif

namespace mvstore {
std::unordered_map<oid_t, BTree *> database_tables;
std::vector<Catalog *> tpcc_catalogs={};
namespace benchmark {
namespace tpcc {

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////

const size_t name_length = 31;
const size_t middle_name_length = 1;
const size_t data_length = 64;
const size_t state_length = 15;
const size_t zip_length = 8;
const size_t street_length = 31;
const size_t city_length = 31;
const size_t credit_length = 1;
const size_t phone_length = 31;
const size_t dist_length = 31;

double item_min_price = 1.0;
double item_max_price = 100.0;

double warehouse_name_length = 15;
double warehouse_min_tax = 0.0;
double warehouse_max_tax = 0.2;
double warehouse_initial_ytd = 300000.00f;

double district_name_length = 15;
double district_min_tax = 0.0;
double district_max_tax = 0.2;
double district_initial_ytd = 30000.00f;

std::string customers_good_credit = "GC";
std::string customers_bad_credit = "BC";
double customers_bad_credit_ratio = 0.1;
double customers_init_credit_lim = 50000.0;
double customers_min_discount = 0;
double customers_max_discount = 0.5;
double customers_init_balance = -10.0;
double customers_init_ytd = 10.0;
int customers_init_payment_cnt = 1;
int customers_init_delivery_cnt = 0;

double history_init_amount = 10.0;
size_t history_data_length = 32;

int orders_min_ol_cnt = 5;
int orders_max_ol_cnt = 15;
int orders_init_all_local = 1;
int orders_null_carrier_id = 0;
int orders_min_carrier_id = 1;
int orders_max_carrier_id = 10;

int new_orders_per_district = 900;  // 900

int order_line_init_quantity = 5;
int order_line_max_ol_quantity = 10;
double order_line_min_amount = 0.01;
size_t order_line_dist_info_length = 32;

double stock_original_ratio = 0.1;
int stock_min_quantity = 10;
int stock_max_quantity = 100;
int stock_dist_count = 10;

double payment_min_amount = 1.0;
double payment_max_amount = 5000.0;

int stock_min_threshold = 10;
int stock_max_threshold = 20;

double new_order_remote_txns = 0.01;

const int syllable_count = 10;
const char *syllables[syllable_count] = {"BAR", "OUGHT", "ABLE", "PRI",
                                         "PRES", "ESES", "ANTI", "CALLY",
                                         "ATION", "EING"};

const std::string data_constant = std::string("FOO");

NURandConstant nu_rand_const;



/////////////////////////////////////////////////////////
// Globals
/////////////////////////////////////////////////////////

//#define THREAD_POOL_SIZE 16
//sync::ThreadPool the_tp(THREAD_POOL_SIZE);

/////////////////////////////////////////////////////////
// Create the tables
/////////////////////////////////////////////////////////

WarehouseTable *warehouse_table;
DistrictTable *district_table;
ItemTable *item_table;
CustomerTable *customer_table;
HistoryTable *history_table;
StockTable *stock_table;
OrderTable *orders_table;
NewOrderTable *new_order_table;
OrderLineTable *order_line_table;

OrderSKeyTable *orders_skey_table;
CustomerSKeyTable *customer_skey_table;

RegionTable *region_table;
NationTable *nation_table;
SupplierTable *supplier_table;

static std::atomic<int32_t> history_id(1);


void CreateTPCCDatabase(ParameterSet param, VersionStore *buf_mgr ,
                        DramBlockPool *leaf_node_pool ,
                        InnerNodeBuffer *inner_node_pool ,
                        EphemeralPool *conflict_buffer) {
    // Clean up
    warehouse_table = nullptr;
    district_table = nullptr;
    item_table = nullptr;
    history_table = nullptr;
    history_table = nullptr;
    stock_table = nullptr;
    orders_table = nullptr;
    new_order_table = nullptr;
    order_line_table = nullptr;
    region_table = nullptr;
    nation_table = nullptr;
    supplier_table = nullptr;

    std::vector<Catalog *> schemas;
    //Log manager
    Catalog *log_catalog = new Catalog();
    log_catalog->table_id = 0;
    log_catalog->is_log = true;
    schemas.push_back(log_catalog);

    //CreateWarehouseTable
    {
        /*
         CREATE TABLE WAREHOUSE (
         W_ID SMALLINT DEFAULT '0' NOT NULL,
         W_NAME VARCHAR(16) DEFAULT NULL,
         W_STREET_1 VARCHAR(32) DEFAULT NULL,
         W_STREET_2 VARCHAR(32) DEFAULT NULL,
         W_CITY VARCHAR(32) DEFAULT NULL,
         W_STATE VARCHAR(2) DEFAULT NULL,
         W_ZIP VARCHAR(9) DEFAULT NULL,
         W_TAX FLOAT DEFAULT NULL,
         W_YTD FLOAT DEFAULT NULL,
         CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
         );
         */
        Catalog *warehouse_catalog = new Catalog();
        uint8_t *key_bitmaps_w = new uint8_t[9];
        key_bitmaps_w[0]=1;

        warehouse_catalog->init("warehouse",9,key_bitmaps_w,8,1,false);
//        warehouse_catalog->add_col("W_ID", 4, "INTEGER");
        warehouse_catalog->add_col("W_ID", 8, "INTEGER");
        warehouse_catalog->add_col("W_NAME", 16, "VARCHAR");
        warehouse_catalog->add_col("W_STREET_1", 32, "VARCHAR");
        warehouse_catalog->add_col("W_STREET_2", 32, "VARCHAR");
        warehouse_catalog->add_col("W_CITY", 32, "VARCHAR");
        warehouse_catalog->add_col("W_STATE", 2, "VARCHAR");
        warehouse_catalog->add_col("W_ZIP", 9, "VARCHAR");
        warehouse_catalog->add_col("W_TAX", 8, "VARCHAR");//double
        warehouse_catalog->add_col("W_YTD", 8, "VARCHAR");//double
        tpcc_catalogs.push_back(warehouse_catalog);
        schemas.push_back(warehouse_catalog);

        uint32_t payload_sz = Warehouse::GetPayloadSize();
        param.payload_size = payload_sz;
        warehouse_table = new WarehouseTable(param, *warehouse_catalog,
                                        leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(0, warehouse_table));
    }

    //CreateDistrictTable
    {
        /*
         CREATE TABLE DISTRICT (
         D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
         D_ID TINYINT DEFAULT '0' NOT NULL,
         D_NAME VARCHAR(16) DEFAULT NULL,
         D_STREET_1 VARCHAR(32) DEFAULT NULL,
         D_STREET_2 VARCHAR(32) DEFAULT NULL,
         D_CITY VARCHAR(32) DEFAULT NULL,
         D_STATE VARCHAR(2) DEFAULT NULL,
         D_ZIP VARCHAR(9) DEFAULT NULL,
         D_TAX FLOAT DEFAULT NULL,
         D_YTD FLOAT DEFAULT NULL,
         D_NEXT_O_ID INT DEFAULT NULL,
         PRIMARY KEY (D_W_ID,D_ID)
         );
         */
        Catalog *district_catalog = new Catalog();
        uint8_t *key_bitmaps_dis = new uint8_t[11];
        key_bitmaps_dis[0]=1;
        key_bitmaps_dis[1]=1;

        district_catalog->init("district",11,key_bitmaps_dis,16,2,false);
//        district_catalog->add_col("D_W_ID", 4, "INTEGER");
//        district_catalog->add_col("D_ID", 4, "INTEGER");
        district_catalog->add_col("D_W_ID", 8, "INTEGER");
        district_catalog->add_col("D_ID", 8, "INTEGER");
        district_catalog->add_col("D_NEXT_O_ID", 4, "INTEGER");
        district_catalog->add_col("D_NAME", 16, "VARCHAR");
        district_catalog->add_col("D_STREET_1", 32, "VARCHAR");
        district_catalog->add_col("D_STREET_2", 32, "VARCHAR");
        district_catalog->add_col("D_CITY", 32, "VARCHAR");
        district_catalog->add_col("D_STATE", 2, "VARCHAR");
        district_catalog->add_col("D_ZIP", 9, "VARCHAR");
        district_catalog->add_col("D_TAX", 8, "VARCHAR");//double
        district_catalog->add_col("D_YTD", 8, "VARCHAR");//double

        tpcc_catalogs.push_back(district_catalog);
        schemas.push_back(district_catalog);

        uint32_t payload_sz = District::GetPayloadSize();
        param.payload_size = payload_sz;
        district_table = new DistrictTable(param, *district_catalog,
                                             leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(1, district_table));
    }

    //CreateItemTable
    {
        /*
         CREATE TABLE ITEM (
         I_ID INTEGER DEFAULT '0' NOT NULL,
         I_IM_ID INTEGER DEFAULT NULL,
         I_NAME VARCHAR(32) DEFAULT NULL,
         I_PRICE FLOAT DEFAULT NULL,
         I_DATA VARCHAR(64) DEFAULT NULL,
         CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
         );
         */
        Catalog *item_catalog = new Catalog();
        uint8_t *key_bitmaps_i = new uint8_t[5];
        key_bitmaps_i[0]=1;

        item_catalog->init("item",5,key_bitmaps_i,8,3,false);
//        item_catalog->add_col("I_ID", 4, "INTEGER");
        item_catalog->add_col("I_ID", 8, "INTEGER");
        item_catalog->add_col("I_IM_ID", 4, "INTEGER");
        item_catalog->add_col("I_NAME", 32, "VARCHAR");
        item_catalog->add_col("I_PRICE", 8, "VARCHAR");//double
        item_catalog->add_col("I_DATA", 64, "VARCHAR");
        tpcc_catalogs.push_back(item_catalog);
        schemas.push_back(item_catalog);

        uint32_t payload_sz = Item::GetPayloadSize();
        param.payload_size = payload_sz;
        item_table = new ItemTable(param, *item_catalog,
                                           leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(2, item_table));
    }

    //CreateCustomerTable
    {
        /*
         CREATE TABLE CUSTOMER (
         C_W_ID SMALLINT DEFAULT '0' NOT NULL,
         C_D_ID TINYINT DEFAULT '0' NOT NULL,
         C_ID INTEGER DEFAULT '0' NOT NULL,
         C_FIRST VARCHAR(32) DEFAULT NULL,
         C_MIDDLE VARCHAR(2) DEFAULT NULL,
         C_LAST VARCHAR(32) DEFAULT NULL,
         C_STREET_1 VARCHAR(32) DEFAULT NULL,
         C_STREET_2 VARCHAR(32) DEFAULT NULL,
         C_CITY VARCHAR(32) DEFAULT NULL,
         C_STATE VARCHAR(2) DEFAULT NULL,
         C_ZIP VARCHAR(9) DEFAULT NULL,
         C_PHONE VARCHAR(32) DEFAULT NULL,
         C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
         C_CREDIT VARCHAR(2) DEFAULT NULL,
         C_CREDIT_LIM FLOAT DEFAULT NULL,
         C_DISCOUNT FLOAT DEFAULT NULL,
         C_BALANCE FLOAT DEFAULT NULL,
         C_YTD_PAYMENT FLOAT DEFAULT NULL,
         C_PAYMENT_CNT INTEGER DEFAULT NULL,
         C_DELIVERY_CNT INTEGER DEFAULT NULL,
         C_DATA VARCHAR(500),
         PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
         UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
         CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID,
         D_W_ID)
         );
         CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
       */
        Catalog *customer_catalog = new Catalog();
        uint8_t *key_bitmaps_c = new uint8_t[21];
        key_bitmaps_c[0]=1;
        key_bitmaps_c[1]=1;
        key_bitmaps_c[2]=1;

        customer_catalog->init("customer",21,key_bitmaps_c,24,4,false);
//        customer_catalog->add_col("C_W_ID", 4, "INTEGER");
//        customer_catalog->add_col("C_D_ID", 4, "INTEGER");
//        customer_catalog->add_col("C_ID", 4, "INTEGER");
        customer_catalog->add_col("C_W_ID", 8, "INTEGER");
        customer_catalog->add_col("C_D_ID", 8, "INTEGER");
        customer_catalog->add_col("C_ID", 8, "INTEGER");
        customer_catalog->add_col("C_SINCE", 8, "LONG INTEGER");//timestamp
        customer_catalog->add_col("C_PAYMENT_CNT", 4, "INTEGER");
        customer_catalog->add_col("C_DELIVERY_CNT", 4, "INTEGER");
        customer_catalog->add_col("C_FIRST", 32, "VARCHAR");
        customer_catalog->add_col("C_MIDDLE", 2, "VARCHAR");
        customer_catalog->add_col("C_LAST", 32, "VARCHAR");
        customer_catalog->add_col("C_STREET_1", 32, "VARCHAR");
        customer_catalog->add_col("C_STREET_2", 32, "VARCHAR");
        customer_catalog->add_col("C_CITY", 32, "VARCHAR");
        customer_catalog->add_col("C_STATE", 2, "VARCHAR");
        customer_catalog->add_col("C_ZIP", 9, "VARCHAR");
        customer_catalog->add_col("C_PHONE", 32, "VARCHAR");
        customer_catalog->add_col("C_CREDIT", 2, "VARCHAR");
        customer_catalog->add_col("C_CREDIT_LIM", 8, "VARCHAR");//float
        customer_catalog->add_col("C_DISCOUNT", 8, "VARCHAR");//float
        customer_catalog->add_col("C_BALANCE", 8, "VARCHAR");//float
        customer_catalog->add_col("C_YTD_PAYMENT", 8, "VARCHAR");//float
        customer_catalog->add_col("C_DATA", 500, "VARCHAR");
        tpcc_catalogs.push_back(customer_catalog);
        schemas.push_back(customer_catalog);

        uint32_t payload_sz = Customer::GetPayloadSize();
        param.payload_size = payload_sz;
        customer_table = new CustomerTable(param, *customer_catalog,
                                       leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(3, customer_table));

    }


    //CreateHistoryTable
    {
        /*
          CREATE TABLE HISTORY (
          H_ID INTEGER DEFAULT NULL,
          H_C_ID INTEGER DEFAULT NULL,
          H_C_D_ID TINYINT DEFAULT NULL,
          H_C_W_ID SMALLINT DEFAULT NULL,
          H_D_ID TINYINT DEFAULT NULL,
          H_W_ID SMALLINT DEFAULT '0' NOT NULL,
          H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
          H_AMOUNT FLOAT DEFAULT NULL,
          H_DATA VARCHAR(32) DEFAULT NULL,
          CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES
          CUSTOMER (C_ID, C_D_ID, C_W_ID),
          CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID,
          D_W_ID)
          );
         */
        Catalog *history_catalog = new Catalog();
        uint8_t *key_bitmaps_h = new uint8_t[9];
        key_bitmaps_h[0]=1;

        history_catalog->init("history",9,key_bitmaps_h,8,5,false);
//        history_catalog->add_col("H_ID", 4, "INTEGER");
        history_catalog->add_col("H_ID", 8, "INTEGER");
        history_catalog->add_col("H_C_ID", 4, "INTEGER");
        history_catalog->add_col("H_C_D_ID", 4, "INTEGER");
        history_catalog->add_col("H_C_W_ID", 4, "INTEGER");
        history_catalog->add_col("H_D_ID", 4, "INTEGER");
        history_catalog->add_col("H_W_ID", 4, "INTEGER");
        history_catalog->add_col("H_DATE", 8, "LONG INTEGER");//timestamp
        history_catalog->add_col("H_AMOUNT", 8, "VARCHAR");//float
        history_catalog->add_col("H_DATA", 32, "VARCHAR");
        tpcc_catalogs.push_back(history_catalog);
        schemas.push_back(history_catalog);

        uint32_t payload_sz = History::GetPayloadSize();
        param.payload_size = payload_sz;
        history_table = new HistoryTable(param, *history_catalog,
                                          leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(4, history_table));
    }

    //CreateStockTable
    {
        /*
         CREATE TABLE STOCK (
         S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
         S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
         S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
         S_YTD INTEGER DEFAULT NULL,
         S_ORDER_CNT INTEGER DEFAULT NULL,
         S_REMOTE_CNT INTEGER DEFAULT NULL,
         S_DIST_01 VARCHAR(32) DEFAULT NULL,
         S_DIST_02 VARCHAR(32) DEFAULT NULL,
         S_DIST_03 VARCHAR(32) DEFAULT NULL,
         S_DIST_04 VARCHAR(32) DEFAULT NULL,
         S_DIST_05 VARCHAR(32) DEFAULT NULL,
         S_DIST_06 VARCHAR(32) DEFAULT NULL,
         S_DIST_07 VARCHAR(32) DEFAULT NULL,
         S_DIST_08 VARCHAR(32) DEFAULT NULL,
         S_DIST_09 VARCHAR(32) DEFAULT NULL,
         S_DIST_10 VARCHAR(32) DEFAULT NULL,
         S_DATA VARCHAR(64) DEFAULT NULL,
         PRIMARY KEY (S_W_ID,S_I_ID)
         );
         */
        Catalog *stock_catalog = new Catalog();
        uint8_t *key_bitmaps_s = new uint8_t[17];
        key_bitmaps_s[0]=1;
        key_bitmaps_s[1]=1;

        stock_catalog->init("stock",17, key_bitmaps_s,16,6,false);
//        stock_catalog->add_col("S_W_ID", 4, "INTEGER");
//        stock_catalog->add_col("S_I_ID", 4, "INTEGER");
        stock_catalog->add_col("S_W_ID", 8, "INTEGER");
        stock_catalog->add_col("S_I_ID", 8, "INTEGER");
        stock_catalog->add_col("S_QUANTITY", 4, "INTEGER");
        stock_catalog->add_col("S_YTD", 4, "INTEGER");
        stock_catalog->add_col("S_ORDER_CNT", 4, "INTEGER");
        stock_catalog->add_col("S_REMOTE_CNT", 4, "INTEGER");
        stock_catalog->add_col("S_DIST_01", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_02", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_03", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_04", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_05", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_06", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_07", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_08", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_09", 32, "VARCHAR");
        stock_catalog->add_col("S_DIST_10", 32, "VARCHAR");
        stock_catalog->add_col("S_DATA", 64, "VARCHAR");
        tpcc_catalogs.push_back(stock_catalog);
        schemas.push_back(stock_catalog);

        uint32_t payload_sz = Stock::GetPayloadSize();
        param.payload_size = payload_sz;
        stock_table = new StockTable(param, *stock_catalog,
                                          leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(5, stock_table));
    }

    //CreateOrdersTable
    {
        /*
         CREATE TABLE ORDERS (
         O_W_ID SMALLINT DEFAULT '0' NOT NULL,
         O_D_ID TINYINT DEFAULT '0' NOT NULL,
         O_ID INTEGER DEFAULT '0' NOT NULL,
         O_C_ID INTEGER DEFAULT NULL,
         O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
         O_CARRIER_ID INTEGER DEFAULT NULL,
         O_OL_CNT INTEGER DEFAULT NULL,
         O_ALL_LOCAL INTEGER DEFAULT NULL,
         PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
         UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
         CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER
         (C_ID, C_D_ID, C_W_ID)
         );
         CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
         */
        Catalog *orders_catalog = new Catalog();
        uint8_t *key_bitmaps_o = new uint8_t[8];
        key_bitmaps_o[0]=1;
        key_bitmaps_o[1]=1;
        key_bitmaps_o[2]=1;

        orders_catalog->init("orders",8, key_bitmaps_o,24,7,false);
//        orders_catalog->add_col("O_W_ID", 4, "INTEGER");
//        orders_catalog->add_col("O_D_ID", 4, "INTEGER");
//        orders_catalog->add_col("O_ID", 4, "INTEGER");
        orders_catalog->add_col("O_W_ID", 8, "INTEGER");
        orders_catalog->add_col("O_D_ID", 8, "INTEGER");
        orders_catalog->add_col("O_ID", 8, "INTEGER");
        orders_catalog->add_col("O_C_ID", 4, "INTEGER");
        orders_catalog->add_col("O_ENTRY_D", 8, "LONG INTEGER");//timestamp
        orders_catalog->add_col("O_CARRIER_ID", 4, "INTEGER");
        orders_catalog->add_col("O_OL_CNT", 4, "INTEGER");
        orders_catalog->add_col("O_ALL_LOCAL", 4, "INTEGER");
        tpcc_catalogs.push_back(orders_catalog);
        schemas.push_back(orders_catalog);

        uint32_t payload_sz = Order::GetPayloadSize();
        param.payload_size = payload_sz;
        orders_table = new OrderTable(param, *orders_catalog,
                                     leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(6, orders_table));
    }

    //CreateNewOrderTable
    {
        /*
         CREATE TABLE NEW_ORDER (
         NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
         NO_D_ID TINYINT DEFAULT '0' NOT NULL,
         NO_O_ID INTEGER DEFAULT '0' NOT NULL,
         CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
         CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES
         ORDERS (O_ID, O_D_ID, O_W_ID)
         );
         */
        Catalog *new_order_catalog = new Catalog();
        uint8_t *key_bitmaps_no = new uint8_t[3];
        key_bitmaps_no[0]=1;
        key_bitmaps_no[1]=1;
        key_bitmaps_no[2]=1;
        new_order_catalog->init("new order",3, key_bitmaps_no,24,8,false);
//        new_order_catalog->add_col("NO_W_ID", 4, "INTEGER");
//        new_order_catalog->add_col("NO_D_ID", 4, "INTEGER");
//        new_order_catalog->add_col("NO_O_ID", 4, "INTEGER");
        new_order_catalog->add_col("NO_W_ID", 8, "INTEGER");
        new_order_catalog->add_col("NO_D_ID", 8, "INTEGER");
        new_order_catalog->add_col("NO_O_ID", 8, "INTEGER");
        tpcc_catalogs.push_back(new_order_catalog);
        schemas.push_back(new_order_catalog);

        uint32_t payload_sz = NewOrder::GetPayloadSize();
        param.payload_size = payload_sz;
        new_order_table = new NewOrderTable(param, *new_order_catalog,
                                      leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(7, new_order_table));
    }

    //CreateOrderLineTable
    {
        /*
         CREATE TABLE ORDER_LINE (
         OL_W_ID INTEGER DEFAULT '0' NOT NULL,
         OL_D_ID TINYINT DEFAULT '0' NOT NULL,
         OL_O_ID SMALLINT DEFAULT '0' NOT NULL,
         OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
         OL_I_ID INTEGER DEFAULT NULL,
         OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
         OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
         OL_QUANTITY INTEGER DEFAULT NULL,
         OL_AMOUNT FLOAT DEFAULT NULL,
         OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
         PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
         CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES
         ORDERS (O_ID, O_D_ID, O_W_ID),
         CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK
         (S_I_ID, S_W_ID)
         );
         CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
         */
        Catalog *order_line_catalog = new Catalog();
        uint8_t *key_bitmaps_ol = new uint8_t[10];
        key_bitmaps_ol[0]=1;
        key_bitmaps_ol[1]=1;
        key_bitmaps_ol[2]=1;
        key_bitmaps_ol[3]=1;
        order_line_catalog->init("order line",10, key_bitmaps_ol,32,9,false);
//        order_line_catalog->add_col("OL_W_ID", 4, "INTEGER");
//        order_line_catalog->add_col("OL_D_ID", 4, "INTEGER");
//        order_line_catalog->add_col("OL_O_ID", 4, "INTEGER");
//        order_line_catalog->add_col("OL_NUMBER", 4, "INTEGER");
        order_line_catalog->add_col("OL_W_ID", 8, "INTEGER");
        order_line_catalog->add_col("OL_D_ID", 8, "INTEGER");
        order_line_catalog->add_col("OL_O_ID", 8, "INTEGER");
        order_line_catalog->add_col("OL_NUMBER", 8, "INTEGER");
        order_line_catalog->add_col("OL_I_ID", 4, "INTEGER");
        order_line_catalog->add_col("OL_SUPPLY_W_ID", 4, "INTEGER");
        order_line_catalog->add_col("OL_DELIVERY_D", 8, "LONG INTEGER");//timestamp
        order_line_catalog->add_col("OL_QUANTITY", 4, "INTEGER");
        order_line_catalog->add_col("OL_AMOUNT", 8, "VARCHAR");//float
        order_line_catalog->add_col("OL_DIST_INFO", 32, "VARCHAR");
        tpcc_catalogs.push_back(order_line_catalog);
        schemas.push_back(order_line_catalog);

        uint32_t payload_sz = OrderLine::GetPayloadSize();
        param.payload_size = payload_sz;
        order_line_table = new OrderLineTable(param, *order_line_catalog,
                                            leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(8, order_line_table));
    }


    //CreateRegionTable
    {
        /*
         CREATE TABLE region (
             r_regionkey int       NOT NULL,
             r_name      char(55)  NOT NULL,
             r_comment   char(152) NOT NULL,
             PRIMARY KEY (r_regionkey)
         );
         */
        Catalog *region_catalog = new Catalog();
        uint8_t *key_bitmaps_i = new uint8_t[3];
        key_bitmaps_i[0]=1;

        region_catalog->init("region",3,key_bitmaps_i,8,10,false);
        region_catalog->add_col("R_REGIONKEY", 8, "INTEGER");
        region_catalog->add_col("R_NAME", 55, "VARCHAR");
        region_catalog->add_col("R_COMMENT", 152, "VARCHAR");

        tpcc_catalogs.push_back(region_catalog);
        schemas.push_back(region_catalog);

        uint32_t payload_sz = Region::GetPayloadSize();
        param.payload_size = payload_sz;
        region_table = new RegionTable(param, *region_catalog,
                                   leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(9, region_table));
    }

    //CreateNationTable
    {
        /*
        CREATE TABLE nation (
            n_nationkey int       NOT NULL,
            n_regionkey int       NOT NULL,
            n_name      char(25)  NOT NULL,
            n_comment   char(152) NOT NULL,
            FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey) ON DELETE CASCADE,
            PRIMARY KEY (n_nationkey)
        );
         */
        Catalog *nation_catalog = new Catalog();
        uint8_t *key_bitmaps_i = new uint8_t[4];
        key_bitmaps_i[0]=1;

        nation_catalog->init("nation",4,key_bitmaps_i,8,11,false);
        nation_catalog->add_col("N_NATIONKEY", 8, "INTEGER");
        nation_catalog->add_col("N_REGIONKEY", 8, "INTEGER");
        nation_catalog->add_col("N_NAME", 25, "VARCHAR");
        nation_catalog->add_col("N_COMMENT", 152, "VARCHAR");

        tpcc_catalogs.push_back(nation_catalog);
        schemas.push_back(nation_catalog);

        uint32_t payload_sz = Nation::GetPayloadSize();
        param.payload_size = payload_sz;
        nation_table = new RegionTable(param, *nation_catalog,
                                       leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(10, nation_table));
    }

    //CreateSupplierTable
    {
        /*
        CREATE TABLE supplier (
            su_suppkey   int            NOT NULL,
            su_nationkey int            NOT NULL,
            su_acctbal   numeric(12, 2) NOT NULL,
            su_name      char(25)       NOT NULL,
            su_address   varchar(40)    NOT NULL,
            su_phone     char(15)       NOT NULL,
            su_comment   char(101)      NOT NULL,
            FOREIGN KEY (su_nationkey) REFERENCES nation (n_nationkey) ON DELETE CASCADE,
            PRIMARY KEY (su_suppkey)
        );
         */
        Catalog *supplier_catalog = new Catalog();
        uint8_t *key_bitmaps_i = new uint8_t[7];
        key_bitmaps_i[0]=1;

        supplier_catalog->init("supplier",7,key_bitmaps_i,8,12,false);
        supplier_catalog->add_col("SU_SUPPKEY", 8, "INTEGER");
        supplier_catalog->add_col("SU_NATIONKEY", 8, "INTEGER");
        supplier_catalog->add_col("SU_ACCTBAL", 8, "VARCHAR");//double
        supplier_catalog->add_col("SU_NAME", 25, "VARCHAR");
        supplier_catalog->add_col("SU_ADDRESS", 40, "VARCHAR");
        supplier_catalog->add_col("SU_PHONE", 15, "VARCHAR");
        supplier_catalog->add_col("SU_COMMENT", 15, "VARCHAR");

        tpcc_catalogs.push_back(supplier_catalog);
        schemas.push_back(supplier_catalog);

        uint32_t payload_sz = Supplier::GetPayloadSize();
        param.payload_size = payload_sz;
        supplier_table = new SupplierTable(param, *supplier_catalog,
                                       leaf_node_pool, inner_node_pool, conflict_buffer);
        database_tables.insert(std::make_pair(11, supplier_table));
    }

    //CreateCustomerSkeyTable
//    {
//        /*
//         CREATE TABLE CUSTOMER_SKEY (
//         C_ID INTEGER DEFAULT '0' NOT NULL,
//          C_W_ID SMALLINT DEFAULT '0' NOT NULL,
//         C_D_ID TINYINT DEFAULT '0' NOT NULL,
//         C_LAST VARCHAR(32) DEFAULT NULL)
//         */
//        Catalog *customer_skey_catalog = new Catalog();
//        uint8_t *key_bitmaps_c_s = new uint8_t[4];
//        key_bitmaps_c_s[0]=1;
//        key_bitmaps_c_s[1]=1;
//        key_bitmaps_c_s[2]=1;
//        key_bitmaps_c_s[3]=1;
//        customer_skey_catalog->init("customer_skey",4,key_bitmaps_c_s,56,10,false);
////        customer_skey_catalog->add_col("C_ID", 4, "INTEGER");
////        customer_skey_catalog->add_col("C_W_ID", 4, "INTEGER");
////        customer_skey_catalog->add_col("C_D_ID", 4, "INTEGER");
//        customer_skey_catalog->add_col("C_ID", 8, "INTEGER");
//        customer_skey_catalog->add_col("C_W_ID", 8, "INTEGER");
//        customer_skey_catalog->add_col("C_D_ID", 8, "INTEGER");
//        customer_skey_catalog->add_col("C_LAST", 32, "VARCHAR");
//        tpcc_catalogs.push_back(customer_skey_catalog);
//        schemas.push_back(*customer_skey_catalog);
//
//        uint32_t payload_sz = CustomerIndexedColumns::GetPayloadSize();
//        param.payload_size = payload_sz;
//        customer_skey_table = new CustomerSKeyTable(param, *customer_skey_catalog,
//                                              leaf_node_pool, inner_node_pool, conflict_buffer);
//        database_tables.insert(std::make_pair(9, customer_skey_table));
//    }

    //CreateOrdersSkeyTable
//    {
//        /*
//         CREATE TABLE ORDERS (
//         O_ID INTEGER DEFAULT '0' NOT NULL,
//         O_C_ID INTEGER DEFAULT NULL,
//         O_D_ID TINYINT DEFAULT '0' NOT NULL,
//         O_W_ID SMALLINT DEFAULT '0' NOT NULL)
//         */
//        Catalog *orders_skey_catalog = new Catalog();
//        uint8_t *key_bitmaps_o_s = new uint8_t[4];
//        key_bitmaps_o_s[0]=1;
//        key_bitmaps_o_s[1]=1;
//        key_bitmaps_o_s[2]=1;
//        key_bitmaps_o_s[3]=1;
//        orders_skey_catalog->init("orders_skey",4,key_bitmaps_o_s,32,11,false);
////        orders_skey_catalog->add_col("O_ID", 4, "INTEGER");
////        orders_skey_catalog->add_col("O_C_ID", 4, "INTEGER");
////        orders_skey_catalog->add_col("O_D_ID", 4, "INTEGER");
////        orders_skey_catalog->add_col("O_W_ID", 4, "INTEGER");
//        orders_skey_catalog->add_col("O_ID", 8, "INTEGER");
//        orders_skey_catalog->add_col("O_C_ID", 8, "INTEGER");
//        orders_skey_catalog->add_col("O_D_ID", 8, "INTEGER");
//        orders_skey_catalog->add_col("O_W_ID", 8, "INTEGER");
//        tpcc_catalogs.push_back(orders_skey_catalog);
//        schemas.push_back(*orders_skey_catalog);
//
//        uint32_t payload_sz = OrderIndexedColumns::GetPayloadSize();
//        param.payload_size = payload_sz;
//        orders_skey_table = new OrderSKeyTable(param, *orders_skey_catalog,
//                                                    leaf_node_pool, inner_node_pool, conflict_buffer);
//        database_tables.insert(std::make_pair(10, orders_skey_table));
//    }



    buf_mgr->Init(schemas);
}

/////////////////////////////////////////////////////////
// Load in the tables
/////////////////////////////////////////////////////////

std::random_device rd;
std::mt19937 rng(rd());

// Create random NURand constants, appropriate for loading the database.
NURandConstant::NURandConstant() {
    c_last = GetRandomInteger(0, 255);
    c_id = GetRandomInteger(0, 1023);
    order_line_itme_id = GetRandomInteger(0, 8191);
}

// A non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
int GetNURand(int a, int x, int y) {
    assert(x <= y);
    int c = nu_rand_const.c_last;

    if (a == 255) {
        c = nu_rand_const.c_last;
    } else if (a == 1023) {
        c = nu_rand_const.c_id;
    } else if (a == 8191) {
        c = nu_rand_const.order_line_itme_id;
    } else {
        assert(false);
    }

    return (((GetRandomInteger(0, a) | GetRandomInteger(x, y)) + c) %
            (y - x + 1)) + x;
}

// A last name as defined by TPC-C 4.3.2.3. Not actually random.
std::string GetLastName(int number) {
    assert(number >= 0 && number <= 999);

    int idx1 = number / 100;
    int idx2 = (number / 10 % 10);
    int idx3 = number % 10;

    char lastname_cstr[name_length];
    std::strcpy(lastname_cstr, syllables[idx1]);
    std::strcat(lastname_cstr, syllables[idx2]);
    std::strcat(lastname_cstr, syllables[idx3]);

    return std::string(lastname_cstr);
}

// A non-uniform random last name, as defined by TPC-C 4.3.2.3.
// The name will be limited to maxCID
std::string GetRandomLastName(int max_cid) {
    int min_cid = 999;
    if (max_cid - 1 < min_cid) {
        min_cid = max_cid - 1;
    }

    return GetLastName(GetNURand(255, 0, min_cid));
}

std::string GetRandomAlphaNumericString(const size_t string_length) {
    const char alphanumeric[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

    std::uniform_int_distribution<> dist(0, sizeof(alphanumeric) - 1);

    char repeated_char = alphanumeric[dist(rng)];
    std::string sample(string_length, repeated_char);
    return sample;
}

bool GetRandomBoolean(double ratio) {
    double sample = (double) rand() / RAND_MAX;
    return (sample < ratio) ? true : false;
}

int GetRandomInteger(const int lower_bound, const int upper_bound) {
    std::uniform_int_distribution<> dist(lower_bound, upper_bound);

    int sample = dist(rng);
    return sample;
}

int GetRandomIntegerExcluding(const int lower_bound, const int upper_bound,
                          const int exclude_sample) {
    int sample;
    if (lower_bound == upper_bound) return lower_bound;

    while (1) {
        sample = GetRandomInteger(lower_bound, upper_bound);
        if (sample != exclude_sample) break;
    }
    return sample;
}

double GetRandomDouble(const double lower_bound, const double upper_bound) {
    std::uniform_real_distribution<> dist(lower_bound, upper_bound);

    double sample = dist(rng);
    return sample;
}

double GetRandomFixedPoint(int decimal_places, double minimum, double maximum) {
    assert(decimal_places > 0);
    assert(minimum < maximum);

    int multiplier = 1;
    for (int i = 0; i < decimal_places; ++i) {
        multiplier *= 10;
    }

    int int_min = (int) (minimum * multiplier + 0.5);
    int int_max = (int) (maximum * multiplier + 0.5);

    return GetRandomDouble(int_min, int_max) / (double) (multiplier);
}

std::string GetStreetName() {
    std::vector<std::string> street_names = {
            "5835 Alderson St", "117  Ettwein St", "1400 Fairstead Ln",
            "1501 Denniston St", "898  Flemington St", "2325 Eldridge St",
            "924  Lilac St", "4299 Minnesota St", "5498 Northumberland St",
            "5534 Phillips Ave"};

    std::uniform_int_distribution<> dist(0, street_names.size() - 1);
    return street_names[dist(rng)];
}

std::string GetZipCode() {
    std::vector<std::string> zip_codes = {"15215", "14155", "80284", "61845",
                                          "23146", "21456", "12345", "21561",
                                          "87752", "91095"};

    std::uniform_int_distribution<> dist(0, zip_codes.size() - 1);
    return zip_codes[dist(rng)];
}

std::string GetCityName() {
    std::vector<std::string> city_names = {
            "Madison", "Pittsburgh", "New York", "Seattle", "San Francisco",
            "Berkeley", "Palo Alto", "Los Angeles", "Boston", "Redwood Shores"};

    std::uniform_int_distribution<> dist(0, city_names.size() - 1);
    return city_names[dist(rng)];
}

std::string GetStateName() {
    std::vector<std::string> state_names = {"WI", "PA", "NY", "WA", "CA", "MA"};

    std::uniform_int_distribution<> dist(0, state_names.size() - 1);
    return state_names[dist(rng)];
}

int GetTimeStamp() {
    auto time_stamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    return time_stamp;
}
Region BuildRegionTuple(const int region_id){
    Region r;
    r.R_REGIONKEY = region_id;
    //name
    std::string name_ = std::string(regions[region_id]);
    strncpy(r.R_NAME, name_.c_str(), sizeof(r.R_NAME));
    //comment
    auto comment_ = GetRandomAlphaNumericString(data_length);
    strncpy(r.R_COMMENT, comment_.c_str(), sizeof(r.R_COMMENT));

    return r;
}
Nation BuildNationTuple(const int nation_id){
    Nation n;
    n.N_NATIONKEY = nation_id;
    //region key
    n.N_REGIONKEY = nations[nation_id].N_REGIONKEY;
    //name
    std::string name_ = std::string(nations[nation_id].N_NAME);
    strncpy(n.N_NAME, name_.c_str(), sizeof(n.N_NAME));
    //comment
    auto comment_ = GetRandomAlphaNumericString(data_length);
    strncpy(n.N_COMMENT, comment_.c_str(), sizeof(n.N_COMMENT));

    return n;
}
Supplier BuildSupplierTuple(const int supp_id){
    Supplier su;
    su.SU_SUPPKEY = supp_id;
    //nation key
    auto nation_k = GetRandomInteger(0, 61);
    su.SU_NATIONKEY = nation_k;
    //acctbal
    //has nobody use it
    auto accbal_ = GetRandomDouble(warehouse_min_tax, warehouse_max_tax);
    strncpy(su.SU_ACCTBAL, std::to_string(accbal_).c_str(), sizeof(su.SU_ACCTBAL));
    //name
    std::string name_ = std::string("Supplier#") + std::string("000000000") + std::to_string(supp_id);
    strncpy(su.SU_NAME, name_.c_str(), sizeof(su.SU_NAME));
    //address
    auto street_ = GetRandomAlphaNumericString(street_length);
    strncpy(su.SU_ADDRESS, street_.c_str(), sizeof(su.SU_ADDRESS));
    //phone
    auto phone_ = GetRandomAlphaNumericString(phone_length);
    strncpy(su.SU_PHONE, phone_.c_str(), sizeof(su.SU_PHONE));
    //comment
    auto comment_ = GetRandomAlphaNumericString(data_length);
    strncpy(su.SU_COMMENT, comment_.c_str(), sizeof(su.SU_COMMENT));

    return su;
}

Item BuildItemTuple(const int item_id) {
    Item i;
    // I_ID
    i.I_ID = item_id;
    // I_IM_ID
    i.I_IM_ID = item_id * 10;
    // I_NAME
    auto i_name = GetRandomAlphaNumericString(name_length);
    strncpy(i.I_NAME, i_name.c_str(), sizeof(i.I_NAME));
    // I_PRICE
    auto i_price = GetRandomDouble(item_min_price, item_max_price);
    strncpy(i.I_PRICE, std::to_string(i_price).c_str(), sizeof(i.I_PRICE));

    // I_DATA
    auto i_data = GetRandomAlphaNumericString(data_length);
    strncpy(i.I_DATA, i_data.c_str(), sizeof(i.I_DATA));

    return i;
}

Warehouse BuildWarehouseTuple(const int warehouse_id) {
    Warehouse w;

    // W_ID
    w.W_ID = warehouse_id;
    // W_NAME
    auto w_name = GetRandomAlphaNumericString(warehouse_name_length);
    strncpy(w.W_NAME, w_name.c_str(), sizeof(w.W_NAME));
    // W_STREET_1, W_STREET_2
    auto w_street = GetStreetName();
    strncpy(w.W_STREET_1, w_street.c_str(), sizeof(w.W_STREET_1));
    strncpy(w.W_STREET_2, w_street.c_str(), sizeof(w.W_STREET_2));
    // W_CITY
    auto w_city = GetCityName();
    strncpy(w.W_CITY, w_city.c_str(), sizeof(w.W_CITY));
    // W_STATE
    auto w_state = GetStateName();
    strncpy(w.W_STATE, w_state.c_str(), sizeof(w.W_STATE));
    // W_ZIP
    auto w_zip = GetZipCode();
    strncpy(w.W_ZIP, w_zip.c_str(), sizeof(w.W_ZIP));
    // W_TAX
    auto w_tax = GetRandomDouble(warehouse_min_tax, warehouse_max_tax);
    strncpy(w.W_TAX, std::to_string(w_tax).c_str(), sizeof(w.W_TAX));
    // W_YTD
    auto w_ytd = warehouse_initial_ytd;
    strncpy(w.W_YTD, std::to_string(w_ytd).c_str(), sizeof(w.W_YTD));
    return w;
}

District BuildDistrictTuple(const int district_id, const int warehouse_id) {
    District d;

    // D_ID
    d.D_ID = district_id;

    // D_W_ID
    d.D_W_ID = warehouse_id;

    // D_NAME
    auto d_name = GetRandomAlphaNumericString(district_name_length);
    strncpy(d.D_NAME, d_name.c_str(), sizeof(d.D_NAME));

    // D_STREET_1, D_STREET_2
    auto d_street = GetStreetName();
    strncpy(d.D_STREET_1, d_street.c_str(), sizeof(d.D_STREET_1));
    strncpy(d.D_STREET_2, d_street.c_str(), sizeof(d.D_STREET_2));

    // D_CITY
    auto d_city = GetCityName();
    strncpy(d.D_CITY, d_city.c_str(), sizeof(d.D_CITY));

    // D_STATE
    auto d_state = GetStateName();
    strncpy(d.D_STATE, d_state.c_str(), sizeof(d.D_STATE));

    // D_ZIP
    auto d_zip = GetZipCode();
    strncpy(d.D_ZIP, d_zip.c_str(), sizeof(d.D_ZIP));

    // D_TAX
    double d_tax_d = GetRandomDouble(district_min_tax, district_max_tax);
//    d.D_TAX = d_tax_d;
    strncpy(d.D_TAX, std::to_string(d_tax_d).c_str(), sizeof(d.D_TAX));

    // D_YTD
//    d.D_YTD = district_initial_ytd;
    strncpy(d.D_YTD, std::to_string(district_initial_ytd).c_str(), sizeof(d.D_YTD));

    // D_NEXT_O_ID
    d.D_NEXT_O_ID = state.customers_per_district + 1;
    return d;
}

Customer BuildCustomerTuple(const int customer_id, const int district_id, const int warehouse_id) {

    // Customer id begins from 0
    assert(customer_id >= 0 && customer_id < state.customers_per_district);

    Customer c;

    memset(&c, 0, sizeof(c));
    // C_ID
    c.C_ID = customer_id;

    // C_D_ID
    c.C_D_ID = district_id;

    // C_W_ID
    c.C_W_ID = warehouse_id;

    // C_FIRST, C_MIDDLE, C_LAST
    auto c_first = GetRandomAlphaNumericString(name_length);

    std::string c_last;

    // Here our customer id begins from 0
    if (customer_id <= 999) {
        c_last = GetLastName(customer_id);
    } else {
        c_last = GetRandomLastName(state.customers_per_district);
    }

    auto c_middle = GetRandomAlphaNumericString(middle_name_length);

    strncpy(c.C_FIRST, c_first.c_str(), sizeof(c.C_FIRST));
    strncpy(c.C_MIDDLE, c_middle.c_str(), sizeof(c.C_MIDDLE));
    strncpy(c.C_LAST, c_last.c_str(), sizeof(c.C_LAST));

    // C_STREET_1, C_STREET_2
    auto c_street = GetStreetName();
    strncpy(c.C_STREET_1, c_street.c_str(), sizeof(c.C_STREET_1));
    strncpy(c.C_STREET_2, c_street.c_str(), sizeof(c.C_STREET_2));

    // C_CITY
    auto c_city = GetCityName();
    strncpy(c.C_CITY, c_city.c_str(), sizeof(c.C_CITY));

    // C_STATE
    auto c_state = GetStateName();
    strncpy(c.C_STATE, c_state.c_str(), sizeof(c.C_STATE));

    // C_ZIP
    auto c_zip = GetZipCode();
    strncpy(c.C_ZIP, c_zip.c_str(), sizeof(c.C_ZIP));

    // C_PHONE
    auto c_phone = GetRandomAlphaNumericString(phone_length);
    strncpy(c.C_PHONE, c_phone.c_str(), sizeof(c.C_PHONE));

    // C_SINCE_TIMESTAMP
    auto c_since_timestamp = GetTimeStamp();
    c.C_SINCE = GetTimeStamp();

    // C_CREDIT
    auto c_bad_credit = GetRandomBoolean(customers_bad_credit_ratio);
    auto c_credit = c_bad_credit ? customers_bad_credit : customers_good_credit;
    memcpy(c.C_CREDIT, c_credit.data(), sizeof(c.C_CREDIT));

    // C_CREDIT_LIM
    auto c_credit_lim = customers_init_credit_lim;
    strncpy(c.C_CREDIT_LIM, std::to_string(c_credit_lim).c_str(), sizeof(c.C_CREDIT_LIM));

    // C_DISCOUNT
    auto c_discount = GetRandomDouble(customers_min_discount, customers_max_discount);
    strncpy(c.C_DISCOUNT, std::to_string(c_discount).c_str(), sizeof(c.C_DISCOUNT));

    // C_BALANCE
    auto c_balance = customers_init_balance;
    strncpy(c.C_BALANCE, std::to_string(c_balance).c_str(), sizeof(c.C_BALANCE));

    // C_YTD_PAYMENT
    auto c_ytd_payment = customers_init_ytd;
    strncpy(c.C_YTD_PAYMENT, std::to_string(c_ytd_payment).c_str(), sizeof(c.C_YTD_PAYMENT));

    // C_PAYMENT_CNT
    c.C_PAYMENT_CNT = customers_init_payment_cnt;

    // C_DELIVERY_CNT
    c.C_DELIVERY_CNT = customers_init_delivery_cnt;

    // C_DATA
    auto c_data = GetRandomAlphaNumericString(data_length);
    strncpy(c.C_DATA, c_data.data(), sizeof(c.C_DATA));

    return c;
}

History BuildHistoryTuple(const int customer_id, const int district_id, const int warehouse_id,
                      const int history_district_id, const int history_warehouse_id) {
    History h;
    h.H_ID = history_id.fetch_add(1);
    // H_C_ID
    h.H_C_ID = customer_id;

    // H_C_D_ID
    h.H_C_D_ID = district_id;

    // H_C_W_ID
    h.H_C_W_ID = warehouse_id;

    // H_D_ID
    h.H_D_ID = history_district_id;

    // H_W_ID
    h.H_W_ID = history_warehouse_id;

    // H_DATE
    auto h_date = GetTimeStamp();
    h.H_DATE = h_date;

    // H_AMOUNT
    auto h_amount = history_init_amount;
    strncpy(h.H_AMOUNT, std::to_string(h_amount).c_str(), sizeof(h.H_AMOUNT));

    // H_DATA
    auto h_data = GetRandomAlphaNumericString(history_data_length);
    memcpy(h.H_DATA, h_data.data(), sizeof(h.H_DATA));

    return h;
}

Order BuildOrdersTuple(const int orders_id,
                   const int district_id,
                   const int warehouse_id,
                   const bool new_order,
                   const int o_ol_cnt) {
    Order o;

    // O_ID
    o.O_ID = orders_id;

    // O_C_ID
    auto o_c_id = GetRandomInteger(0, state.customers_per_district);
    o.O_C_ID = o_c_id;

    // O_D_ID
    o.O_D_ID = district_id;

    // O_W_ID
    o.O_W_ID = warehouse_id;

    // O_ENTRY_D
    auto o_entry_d = GetTimeStamp();
    o.O_ENTRY_D = o_entry_d;

    // O_CARRIER_ID
    auto o_carrier_id = orders_null_carrier_id;
    if (new_order == false) {
        o_carrier_id =
                GetRandomInteger(orders_min_carrier_id, orders_max_carrier_id);
    }
    o.O_CARRIER_ID = o_carrier_id;

    // O_OL_CNT
    o.O_OL_CNT = o_ol_cnt;

    // O_ALL_LOCAL
    o.O_ALL_LOCAL = orders_init_all_local;

    return o;
}

NewOrder BuildNewOrderTuple(const int orders_id,
                        const int district_id,
                        const int warehouse_id) {
    NewOrder no;

    // NO_O_ID
    no.NO_O_ID = orders_id;

    // NO_D_ID
    no.NO_D_ID = district_id;

    // NO_W_ID
    no.NO_W_ID = warehouse_id;

    return no;
}

OrderLine BuildOrderLineTuple(const int orders_id, const int district_id, const int warehouse_id,
                          const int order_line_id, const int ol_supply_w_id, const bool new_order) {
    OrderLine ol;

    // OL_O_ID
    ol.OL_O_ID = orders_id;

    // OL_D_ID
    ol.OL_D_ID = district_id;

    // OL_W_ID
    ol.OL_W_ID = warehouse_id;

    // OL_NUMBER
    ol.OL_NUMBER = order_line_id;

    // OL_I_ID
    auto ol_i_id = GetRandomInteger(0, state.item_count);
    ol.OL_I_ID = ol_i_id;

    // OL_SUPPLY_W_ID
    ol.OL_SUPPLY_W_ID = ol_supply_w_id;

    // OL_DELIVERY_D
    int64_t ol_delivery_d = GetTimeStamp();
    if (new_order == true) {
        ol_delivery_d = std::numeric_limits<int64_t>::min();
    }
    ol.OL_DELIVERY_D = ol_delivery_d;

    // OL_QUANTITY
    ol.OL_QUANTITY = order_line_init_quantity;

    // OL_AMOUNT
    double ol_amount = 0;
    if (new_order == true) {
        ol_amount = GetRandomDouble(order_line_min_amount,
                                    order_line_max_ol_quantity * item_max_price);
    }
//    ol.OL_AMOUNT = ol_amount;
    strncpy(ol.OL_AMOUNT, std::to_string(ol_amount).c_str(), sizeof(ol.OL_AMOUNT));

    // OL_DIST_INFO
    auto ol_dist_info = GetRandomAlphaNumericString(order_line_dist_info_length);
    memcpy(ol.OL_DIST_INFO, ol_dist_info.c_str(), sizeof(ol.OL_DIST_INFO));

    return ol;
}

Stock BuildStockTuple(const int stock_id, const int s_w_id) {
    Stock s;

    // S_I_ID
    s.S_I_ID = stock_id;

    // S_W_ID
    s.S_W_ID = s_w_id;

    // S_QUANTITY
    auto s_quantity = GetRandomInteger(stock_min_quantity, stock_max_quantity);
    s.S_QUANTITY = s_quantity;

    // S_YTD
    auto s_ytd = 0;
    s.S_YTD = s_ytd;

    // S_ORDER_CNT
    auto s_order_cnt = 0;
    s.S_ORDER_CNT = s_order_cnt;

    // S_REMOTE_CNT
    auto s_remote_cnt = 0;
    s.S_REMOTE_CNT = s_remote_cnt;

    // S_DIST_01 .. S_DIST_10
    auto s_dist = GetRandomAlphaNumericString(name_length);
    strncpy(s.S_DIST_01, s_dist.c_str(), sizeof(s.S_DIST_01));
    strncpy(s.S_DIST_02, s_dist.c_str(), sizeof(s.S_DIST_02));
    strncpy(s.S_DIST_03, s_dist.c_str(), sizeof(s.S_DIST_03));
    strncpy(s.S_DIST_04, s_dist.c_str(), sizeof(s.S_DIST_04));
    strncpy(s.S_DIST_05, s_dist.c_str(), sizeof(s.S_DIST_05));
    strncpy(s.S_DIST_06, s_dist.c_str(), sizeof(s.S_DIST_06));
    strncpy(s.S_DIST_07, s_dist.c_str(), sizeof(s.S_DIST_07));
    strncpy(s.S_DIST_08, s_dist.c_str(), sizeof(s.S_DIST_08));
    strncpy(s.S_DIST_09, s_dist.c_str(), sizeof(s.S_DIST_09));
    strncpy(s.S_DIST_10, s_dist.c_str(), sizeof(s.S_DIST_10));


    // S_DATA
    auto s_data = GetRandomAlphaNumericString(data_length);
    memcpy(s.S_DATA, s_data.c_str(), sizeof(s.S_DATA));

    return s;
}
void LoadRegion(VersionStore *version_store, int thread_id) {
    std::list<uint64_t> retry_rowids;
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx_reg = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    for (uint i = 0; i < 5; i++) {
        Region region_tuple = BuildRegionTuple(i);

        uint64_t k_ = i;
        const char *k_reg = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Region> executor(region_table,
                                                      k_reg,
                                                    sizeof(uint64_t),
                                                      region_tuple,
                                                      txn_ctx_reg,
                                                    version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (ret_code.IsRetryFailure() || ret_code.IsCASFailure()){
            retry_rowids.push_back(k_);
        }
    }

    auto res = txn_manager->CommitTransaction(txn_ctx_reg);
    assert(res == ResultType::SUCCESS);

    //insert retry
    txn_ctx_reg = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!retry_rowids.empty()) {
        uint64_t k_ = retry_rowids.front();
        Region region_tuple = BuildRegionTuple(k_);

        const char *k_reg = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Region> executor(item_table,
                                                    k_reg ,
                                                    sizeof(uint64_t),
                                                    region_tuple,
                                                    txn_ctx_reg,
                                                    version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            retry_rowids.pop_front();
        }
    }
    res = txn_manager->CommitTransaction(txn_ctx_reg);
    LOG_INFO("region btree insert, current thread id :%d transaction has finished.", thread_id);
    assert(res == ResultType::SUCCESS);

    delete txn_ctx_reg;
    txn_ctx_reg = nullptr;

}
void LoadNation(VersionStore *version_store, int thread_id) {
    std::list<uint64_t> retry_rowids;
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx_nat = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    for (uint i = 0; i < 62; i++) {
        Nation nation_tuple = BuildNationTuple(i);

        uint64_t k_ = i;
        const char *k_nat = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Nation> executor(nation_table,
                                                      k_nat,
                                                      sizeof(uint64_t),
                                                      nation_tuple,
                                                      txn_ctx_nat,
                                                      version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (ret_code.IsRetryFailure() || ret_code.IsCASFailure()){
            retry_rowids.push_back(k_);
        }
    }

    auto res = txn_manager->CommitTransaction(txn_ctx_nat);
    assert(res == ResultType::SUCCESS);

    //insert retry
    txn_ctx_nat = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!retry_rowids.empty()) {
        uint64_t k_ = retry_rowids.front();
        Nation nation_tuple = BuildNationTuple(k_);

        const char *k_nat = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Nation> executor(nation_table,
                                                      k_nat ,
                                                      sizeof(uint64_t),
                                                      nation_tuple,
                                                      txn_ctx_nat,
                                                      version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            retry_rowids.pop_front();
        }
    }
    res = txn_manager->CommitTransaction(txn_ctx_nat);
    LOG_INFO("nation btree insert, current thread id :%d transaction has finished.", thread_id);
    assert(res == ResultType::SUCCESS);

    delete txn_ctx_nat;
    txn_ctx_nat = nullptr;

}
void LoadSupplier(VersionStore *version_store, int thread_id) {
    std::list<uint64_t> retry_rowids;
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx_supp = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    for (uint i = 0; i < 10000; i++) {
        Supplier supplier_tuple = BuildSupplierTuple(i);

        uint64_t k_ = i;
        const char *k_supp = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Supplier> executor(supplier_table,
                                                        k_supp,
                                                      sizeof(uint64_t),
                                                        supplier_tuple,
                                                        txn_ctx_supp,
                                                      version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (ret_code.IsRetryFailure() || ret_code.IsCASFailure()){
            retry_rowids.push_back(k_);
        }
    }

    auto res = txn_manager->CommitTransaction(txn_ctx_supp);
    assert(res == ResultType::SUCCESS);

    //insert retry
    txn_ctx_supp = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!retry_rowids.empty()) {
        uint64_t k_ = retry_rowids.front();
        Supplier supplier_tuple = BuildSupplierTuple(k_);

        const char *k_supp = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Supplier> executor(supplier_table,
                                                      k_supp ,
                                                      sizeof(uint64_t),
                                                      supplier_tuple,
                                                      txn_ctx_supp,
                                                      version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            retry_rowids.pop_front();
        }
    }
    res = txn_manager->CommitTransaction(txn_ctx_supp);
    LOG_INFO("supplier btree insert, current thread id :%d transaction has finished.", thread_id);
    assert(res == ResultType::SUCCESS);

    delete txn_ctx_supp;
    txn_ctx_supp = nullptr;

}
void LoadItems(VersionStore *version_store, int thread_id) {
    std::list<uint64_t> retry_rowids;
    auto txn_manager = SSNTransactionManager::GetInstance();
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

    for (auto item_itr = 0; item_itr < state.item_count; item_itr++) {
        Item item_tuple = BuildItemTuple(item_itr);

        uint64_t k_ = item_itr;
        const char *k_item = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Item> executor(item_table,
                                                k_item,
                                                 sizeof(uint64_t),
                                                 item_tuple,
                                                 txn_ctx,
                                                 version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (ret_code.IsRetryFailure() || ret_code.IsCASFailure()){
            retry_rowids.push_back(k_);
        }
    }
    auto res = txn_manager->CommitTransaction(txn_ctx);
    assert(res == ResultType::SUCCESS);

    //insert retry
    txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!retry_rowids.empty()) {
        uint64_t k_ = retry_rowids.front();
        Item item_tuple = BuildItemTuple(k_);

        const char *k_item = reinterpret_cast<const char *>(&k_);
        InsertExecutor<const char *, Item> executor(item_table,
                                                k_item ,
                                                sizeof(uint64_t),
                                                item_tuple,
                                                txn_ctx,
                                                version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            retry_rowids.pop_front();
        }
    }
    res = txn_manager->CommitTransaction(txn_ctx);
    LOG_INFO("item btree insert, current thread id :%d transaction has finished.", thread_id);
    assert(res == ResultType::SUCCESS);

    delete txn_ctx;
    txn_ctx = nullptr;

}

void LoadWarehouses(VersionStore *version_store, int thread_id,
                const int &warehouse_from, const int &warehouse_to) {
std::list<std::pair<uint64_t ,Warehouse>> w_retry_rowids;
std::list<std::pair<District::DistrictKey,District>> d_retry_rowids;
std::list<std::pair<Customer::CustomerKey,Customer>> c_retry_rowids;
//std::list<std::pair<CustomerIndexedColumns,CustomerIndexedColumns>> c_s_retry_rowids;
std::list<std::pair<uint64_t,History>> h_retry_rowids;
std::list<std::pair<Order::OrderKey,Order>> o_retry_rowids;
//std::list<std::pair<OrderIndexedColumns,OrderIndexedColumns>> o_s_retry_rowids;
std::list<std::pair<NewOrder::NewOrderKey,NewOrder>> n_retry_rowids;
std::list<std::pair<OrderLine::OrderLineKey,OrderLine>> ol_retry_rowids;
std::list<std::pair<Stock::StockKey,Stock>> s_retry_rowids;

auto txn_manager = SSNTransactionManager::GetInstance();

// WAREHOUSES
for (auto warehouse_itr = warehouse_from; warehouse_itr < warehouse_to; warehouse_itr++) {
    auto txn_ctx = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    Warehouse warehouse_tuple = BuildWarehouseTuple(warehouse_itr);
    uint64_t k_w_ = warehouse_itr;
    const char *k_warehouse = reinterpret_cast<const char *>(&k_w_);
    InsertExecutor<const char *, Warehouse> executor_w(warehouse_table,
                                                    k_warehouse,
                                                    sizeof(uint64_t),
                                                    warehouse_tuple,
                                                    txn_ctx,
                                                    version_store);
    auto res_w = executor_w.Execute();
    assert(res_w == true);
    auto ret_code_w = executor_w.GetResultCode();
    if (ret_code_w.IsRetryFailure() || ret_code_w.IsCASFailure()){
        w_retry_rowids.push_back(std::make_pair(warehouse_itr,warehouse_tuple));
    }


    auto txn_res = txn_manager->CommitTransaction(txn_ctx);
    assert(txn_res == ResultType::SUCCESS);


    // DISTRICTS
    for (auto district_itr = 0; district_itr < state.districts_per_warehouse; district_itr++) {
        auto txn_ctx_d = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
        District district_tuple = BuildDistrictTuple(district_itr, warehouse_itr);
        District::DistrictKey k_d_{warehouse_itr,district_itr};
        const char *k_district = reinterpret_cast<const char *>(&k_d_);
        InsertExecutor<const char *, District> executor_d(district_table,
                                                                   k_district,
                                                                   (sizeof(uint64_t)+sizeof(uint64_t)),
                                                                   district_tuple,
                                                                    txn_ctx_d,
                                                                     version_store);
        auto res_d = executor_d.Execute();
        assert(res_d == true);
        auto ret_code_d = executor_d.GetResultCode();
        if (ret_code_d.IsRetryFailure() || ret_code_d.IsCASFailure()){
            d_retry_rowids.push_back(std::make_pair(k_d_,district_tuple));
        }

        auto txn_res_d = txn_manager->CommitTransaction(txn_ctx_d);
        assert(txn_res_d == ResultType::SUCCESS);


        // CUSTOMERS
        for (auto customer_itr = 0; customer_itr < state.customers_per_district; customer_itr++) {
            auto txn_ctx_c_h = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

            Customer customer_tuple = BuildCustomerTuple(customer_itr, district_itr, warehouse_itr);
            {
                Customer::CustomerKey k_c_{warehouse_itr,district_itr,customer_itr};
                const char *k_customer = reinterpret_cast<const char *>(&k_c_);
                InsertExecutor<const char *, Customer> executor_c(customer_table,
                                                                           k_customer,
                                                (sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint64_t)),
                                                           customer_tuple,
                                                                  txn_ctx_c_h,
                                                           version_store);
                auto res_c = executor_c.Execute();
                assert(res_c == true);
                auto ret_code_c = executor_c.GetResultCode();
                if (ret_code_c.IsRetryFailure() || ret_code_c.IsCASFailure()){
                    c_retry_rowids.push_back(std::make_pair(k_c_,customer_tuple));
                }
            }

            // Maintain secondary index as well
//            {
//                CustomerIndexedColumns cic;
//                cic.C_ID = customer_tuple.C_ID;
//                cic.C_W_ID = customer_tuple.C_W_ID;
//                cic.C_D_ID = customer_tuple.C_D_ID;
//                memcpy(cic.C_LAST, customer_tuple.C_LAST, sizeof(cic.C_LAST));
//
//                CustomerIndexedColumns k_c_s_ = cic;
//                uint32_t k_size = sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint64_t)+32;
//                const char *k_customer_index = reinterpret_cast<const char *>(&k_c_s_);
//                InsertExecutor<const char *, CustomerIndexedColumns> executor_c_s(customer_skey_table,
//                                                                                            k_customer_index,
//                                                                                            k_size,
//                                                                                            cic,
//                                                                                            txn_ctx,
//                                                                                            version_store);
//                auto res_c_s = executor_c_s.Execute();
//                assert(res_c_s == true);
//                auto ret_code_c_s = executor_c_s.GetResultCode();
//                if (ret_code_c_s.IsRetryFailure() || ret_code_c_s.IsCASFailure()){
//                    c_s_retry_rowids.push_back(std::make_pair(k_c_s_,cic));
//                }
//            }

            // HISTORY
            int history_district_id = district_itr;
            int history_warehouse_id = warehouse_itr;
            History history_tuple = BuildHistoryTuple(customer_itr, district_itr, warehouse_itr,
                                                        history_district_id, history_warehouse_id);
            {
                uint64_t k_h_ = history_tuple.H_ID;
                const char *k_history = reinterpret_cast<const char *>(&k_h_);
                InsertExecutor<const char *, History> executor_h(history_table,
                                                             k_history,
                                                            sizeof(uint64_t),
                                                            history_tuple,
                                                                 txn_ctx_c_h,
                                                            version_store);


                auto res_h = executor_h.Execute();
                assert(res_h == true);
                auto ret_code_h = executor_h.GetResultCode();
                if (ret_code_h.IsRetryFailure() || ret_code_h.IsCASFailure()){
                    h_retry_rowids.push_back(std::make_pair(k_h_,history_tuple));
                }
            }


            auto txn_res_c_h = txn_manager->CommitTransaction(txn_ctx_c_h);
            assert(txn_res_c_h == ResultType::SUCCESS);

            delete txn_ctx_c_h;
            txn_ctx_c_h = nullptr;
        }  // END CUSTOMERS


        // ORDERS
        for (auto orders_itr = 0; orders_itr < state.customers_per_district; orders_itr++) {
            auto txn_ctx_o_ol = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

            // New order ?
            auto new_order_threshold = state.customers_per_district - new_orders_per_district;
            bool new_order = (orders_itr > new_order_threshold);
            auto o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

            Order orders_tuple = BuildOrdersTuple(orders_itr, district_itr, warehouse_itr, new_order, o_ol_cnt);
            {
                Order::OrderKey k_o_{warehouse_itr,district_itr,orders_itr};
                uint32_t k_o_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
                const char *k_order = reinterpret_cast<const char *>(&k_o_);
                InsertExecutor<const char *, Order> executor_o(orders_table,
                                                                  k_order,
                                                                  k_o_sz,
                                                                  orders_tuple,
                                                               txn_ctx_o_ol,
                                                                  version_store);

                auto res_o = executor_o.Execute();
                assert(res_o == true);
                auto ret_code_o = executor_o.GetResultCode();
                if (ret_code_o.IsRetryFailure() || ret_code_o.IsCASFailure()){
                    o_retry_rowids.push_back(std::make_pair(k_o_,orders_tuple));
                }
            }

            // Maintain secondary index as well
//            {
//                OrderIndexedColumns oic;
//                oic.O_W_ID = orders_tuple.O_W_ID;
//                oic.O_D_ID = orders_tuple.O_D_ID;
//                oic.O_C_ID = orders_tuple.O_C_ID;
//                oic.O_ID = orders_tuple.O_ID;
//                OrderIndexedColumns k_o_s = oic;
//                uint32_t k_o_s_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
//                const char *k_order_index = reinterpret_cast<const char *>(&k_o_s);
//                InsertExecutor<const char *, OrderIndexedColumns> executor_o_s(orders_skey_table,
//                                                                                      k_order_index,
//                                                                                      k_o_s_sz,
//                                                                                      oic,
//                                                                                      txn_ctx,
//                                                                                      version_store);
//                auto res_o_s = executor_o_s.Execute();
//                assert(res_o_s == true);
//                auto ret_code_o_s = executor_o_s.GetResultCode();
//                if (ret_code_o_s.IsRetryFailure() || ret_code_o_s.IsCASFailure()){
//                    o_s_retry_rowids.push_back(std::make_pair(k_o_s,oic));
//                }
//            }

            // NEW_ORDER
            if (new_order) {
                NewOrder new_order_tuple = BuildNewOrderTuple(orders_itr, district_itr, warehouse_itr);
                NewOrder::NewOrderKey k_n_{district_itr,warehouse_itr,orders_itr};
                uint32_t k_n_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
                const char *k_new_order = reinterpret_cast<const char *>(&k_n_);
                InsertExecutor<const char *, NewOrder> executor_n(new_order_table,
                                                                           k_new_order,
                                                                           k_n_sz,
                                                                           new_order_tuple,
                                                                  txn_ctx_o_ol,
                                                                           version_store);

                auto res_n = executor_n.Execute();
                assert(res_n == true);
                auto ret_code_n = executor_n.GetResultCode();
                if (ret_code_n.IsRetryFailure() || ret_code_n.IsCASFailure()){
                    n_retry_rowids.push_back(std::make_pair(k_n_,new_order_tuple));
                }
            }

            // ORDER_LINE
            for (auto order_line_itr = 0; order_line_itr < o_ol_cnt; order_line_itr++) {
                int ol_supply_w_id = warehouse_itr;
                OrderLine order_line_tuple = BuildOrderLineTuple(orders_itr, district_itr, warehouse_itr, order_line_itr,
                                                            ol_supply_w_id, new_order);
                {
                    OrderLine::OrderLineKey k_ol_{warehouse_itr,district_itr,orders_itr,order_line_itr};
                    uint32_t k_ol_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
                    const char *k_order_line = reinterpret_cast<const char *>(&k_ol_);
                    InsertExecutor<const char *, OrderLine> executor_ol(order_line_table,
                                                                                   k_order_line,
                                                                                   k_ol_sz,
                                                                                   order_line_tuple,
                                                                        txn_ctx_o_ol,
                                                                                   version_store);
                    auto res_ol = executor_ol.Execute();
                    assert(res_ol == true);
                    auto ret_code_ol = executor_ol.GetResultCode();
                    if (ret_code_ol.IsRetryFailure() || ret_code_ol.IsCASFailure()){
                        ol_retry_rowids.push_back(std::make_pair(k_ol_,order_line_tuple));
                    }
                }
            }

            auto txn_res_o_ol = txn_manager->CommitTransaction(txn_ctx_o_ol);
            assert(txn_res_o_ol == ResultType::SUCCESS);


            delete txn_ctx_o_ol;
            txn_ctx_o_ol = nullptr;
        }

        delete txn_ctx_d;
        txn_ctx_d = nullptr;
    }  // END DISTRICTS

    // STOCK
    for (auto stock_itr = 0; stock_itr < state.item_count; stock_itr++) {
        auto txn_ctx_s = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);

        int s_w_id = warehouse_itr;
        Stock stock_tuple = BuildStockTuple(stock_itr, s_w_id);
        Stock::StockKey k_s_{s_w_id,stock_itr};
        uint32_t k_s_sz = sizeof(uint64_t) + sizeof(uint64_t);
        const char *k_stock = reinterpret_cast<const char *>(&k_s_);
        InsertExecutor<const char *, Stock> executor_s(stock_table,
                                                          k_stock,
                                                          k_s_sz,
                                                          stock_tuple,
                                                       txn_ctx_s,
                                                          version_store);

        auto res_s = executor_s.Execute();
        assert(res_s == true);
        auto ret_code_s = executor_s.GetResultCode();
        if (ret_code_s.IsRetryFailure() || ret_code_s.IsCASFailure()){
            s_retry_rowids.push_back(std::make_pair(k_s_,stock_tuple));
        }

        auto txn_res_s =txn_manager->CommitTransaction(txn_ctx_s);
        assert(txn_res_s == ResultType::SUCCESS);

        delete txn_ctx_s;
        txn_ctx_s = nullptr;
    }


    delete txn_ctx;
    txn_ctx = nullptr;
}//END WAREHOUSES

//insert retry
{
//    std::list<std::pair<uint32_t,BaseTuple>> w_retry_rowids;
//    std::list<std::pair<District::DistrictKey,BaseTuple>> d_retry_rowids;
//    std::list<std::pair<Customer::CustomerKey,BaseTuple>> c_retry_rowids;
//    std::list<std::pair<CustomerIndexedColumns,BaseTuple>> c_s_retry_rowids;
//    std::list<std::pair<uint32_t,BaseTuple>> h_retry_rowids;
//    std::list<std::pair<Order::OrderKey,BaseTuple>> o_retry_rowids;
//    std::list<std::pair<OrderIndexedColumns,BaseTuple>> o_s_retry_rowids;
//    std::list<std::pair<NewOrder::NewOrderKey,BaseTuple>> n_retry_rowids;
//    std::list<std::pair<OrderLine::OrderLineKey,BaseTuple>> ol_retry_rowids;
//    std::list<std::pair<Stock::StockKey,BaseTuple>> s_retry_rowids;
    auto txn_ctx_rt_w = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!w_retry_rowids.empty()) {
        auto entry_w = w_retry_rowids.front();
        uint64_t k_w = entry_w.first;
        Warehouse t_w = entry_w.second;
        const char *k_warehouse = reinterpret_cast<const char *>(&k_w);
        InsertExecutor<const char *, Warehouse> executor(warehouse_table, k_warehouse,
                                                sizeof(uint64_t),
                                                t_w,
                                                txn_ctx_rt_w,
                                                version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            w_retry_rowids.pop_front();
        }
    }
    auto res_rt_w = txn_manager->CommitTransaction(txn_ctx_rt_w);
    LOG_INFO("LoadWarehouse insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_w == ResultType::SUCCESS);


    auto txn_ctx_rt_d = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!d_retry_rowids.empty()) {
        auto entry_d = d_retry_rowids.front();
        District::DistrictKey k_d = entry_d.first;
        District t_d = entry_d.second;
        const char *k_district = reinterpret_cast<const char *>(&k_d);
//        LOG_INFO("LoadDistrict key D_W_ID:%d,D_ID:%d", k_d.D_W_ID, k_d.D_ID);
        InsertExecutor<const char *, District> executor(district_table,
                                                                   k_district,
                                                                   (sizeof(uint64_t)+sizeof(uint64_t)),
                                                                   t_d,
                                                        txn_ctx_rt_d,
                                                                   version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){

            d_retry_rowids.pop_front();
        }
    }
    auto res_rt_d = txn_manager->CommitTransaction(txn_ctx_rt_d);
    LOG_INFO("LoadDistrict insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_d == ResultType::SUCCESS);


    auto txn_ctx_rt_c = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!c_retry_rowids.empty()) {
        auto entry_c = c_retry_rowids.front();
        Customer::CustomerKey k_c = entry_c.first;
        Customer t_c = entry_c.second;
        const char *k_customer = reinterpret_cast<const char *>(&k_c);
        InsertExecutor<const char *, Customer> executor(customer_table,
                                                                 k_customer,
                                          (sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint64_t)),
                                                                 t_c,
                                                        txn_ctx_rt_c,
                                                                 version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            c_retry_rowids.pop_front();
        }
    }
    auto res_rt_c = txn_manager->CommitTransaction(txn_ctx_rt_c);
    LOG_INFO("LoadCustomer insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_c == ResultType::SUCCESS);
//    while (!c_s_retry_rowids.empty()) {
//        auto entry_c_s = c_s_retry_rowids.front();
//        CustomerIndexedColumns k_c_s = entry_c_s.first;
//        CustomerIndexedColumns t_c_s = entry_c_s.second;
//        uint32_t k_size = sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint64_t)+32;
//        const char *k_customer_index = reinterpret_cast<const char *>(&k_c_s);
//        InsertExecutor<const char *, CustomerIndexedColumns> executor(customer_skey_table,
//                                                                                    k_customer_index,
//                                                                                    k_size,
//                                                                                    t_c_s,
//                                                                                    txn_ctx_rt,
//                                                                                    version_store);
//        auto res = executor.Execute();
//        assert(res == true);
//        auto ret_code = executor.GetResultCode();
//        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
//            c_s_retry_rowids.pop_front();
//        }
//    }
    auto txn_ctx_rt_h = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!h_retry_rowids.empty()) {
        auto entry_h = h_retry_rowids.front();
        uint64_t k_h = entry_h.first;
        History t_h = entry_h.second;
        const char *k_history = reinterpret_cast<const char *>(&k_h);
        InsertExecutor<const char *, History> executor(history_table,
                                                     k_history,
                                                     sizeof(uint64_t),
                                                     t_h,
                                                       txn_ctx_rt_h,
                                                     version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            h_retry_rowids.pop_front();
        }
    }
    auto res_rt_h = txn_manager->CommitTransaction(txn_ctx_rt_h);
    LOG_INFO("LoadHistory insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_h == ResultType::SUCCESS);

    auto txn_ctx_rt_o = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!o_retry_rowids.empty()) {
        auto entry_o = o_retry_rowids.front();
        Order::OrderKey k_o = entry_o.first;
        Order t_o = entry_o.second;
        uint32_t k_o_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
        const char *k_order = reinterpret_cast<const char *>(&k_o);
        InsertExecutor<const char *, Order> executor(orders_table,
                                                          k_order,
                                                          k_o_sz,
                                                          t_o,
                                                     txn_ctx_rt_o,
                                                          version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            o_retry_rowids.pop_front();
        }
    }
    auto res_rt_o = txn_manager->CommitTransaction(txn_ctx_rt_o);
    LOG_INFO("LoadOrder insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_o == ResultType::SUCCESS);
//    while (!o_s_retry_rowids.empty()) {
//        auto entry_o_s = o_s_retry_rowids.front();
//        OrderIndexedColumns k_o_s = entry_o_s.first;
//        OrderIndexedColumns t_o_s = entry_o_s.second;
//        uint32_t k_o_s_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
//        const char *k_order_index = reinterpret_cast<const char *>(&k_o_s);
//        InsertExecutor<const char *, OrderIndexedColumns> executor(orders_skey_table,
//                                                                              k_order_index,
//                                                                              k_o_s_sz,
//                                                                              t_o_s,
//                                                                              txn_ctx_rt,
//                                                                              version_store);
//        auto res = executor.Execute();
//        assert(res == true);
//        auto ret_code = executor.GetResultCode();
//        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
//            o_s_retry_rowids.pop_front();
//        }
//    }
    auto txn_ctx_rt_no = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!n_retry_rowids.empty()) {
        auto entry_n = n_retry_rowids.front();
        NewOrder::NewOrderKey k_n = entry_n.first;
        NewOrder t_n = entry_n.second;
        uint32_t k_n_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
        const char *k_new_order = reinterpret_cast<const char *>(&k_n);
        InsertExecutor<const char *, NewOrder> executor(new_order_table,
                                                                   k_new_order,
                                                                   k_n_sz,
                                                                   t_n,
                                                        txn_ctx_rt_no,
                                                                   version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            n_retry_rowids.pop_front();
        }
    }
    auto res_rt_no = txn_manager->CommitTransaction(txn_ctx_rt_no);
    LOG_INFO("LoadNewOrder insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_no == ResultType::SUCCESS);


    auto txn_ctx_rt_ol = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!ol_retry_rowids.empty()) {
        auto entry_n = ol_retry_rowids.front();
        OrderLine::OrderLineKey k_ol = entry_n.first;
        OrderLine t_ol = entry_n.second;
        uint32_t k_ol_sz = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
        const char *k_new_line = reinterpret_cast<const char *>(&k_ol);
        InsertExecutor<const char *, OrderLine> executor(order_line_table,
                                                                       k_new_line,
                                                                       k_ol_sz,
                                                                       t_ol,
                                                         txn_ctx_rt_ol,
                                                                       version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            ol_retry_rowids.pop_front();
        }
    }
    auto res_rt_ol = txn_manager->CommitTransaction(txn_ctx_rt_ol);
    LOG_INFO("LoadOrderLine insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_ol == ResultType::SUCCESS);


    auto txn_ctx_rt_s = txn_manager->BeginTransaction(thread_id,IsolationLevelType::SERIALIZABLE);
    while (!s_retry_rowids.empty()) {
        auto entry_n = s_retry_rowids.front();
        Stock::StockKey k_s = entry_n.first;
        Stock t_s = entry_n.second;
        uint32_t k_s_sz = sizeof(uint64_t) + sizeof(uint64_t);
        const char *k_stock = reinterpret_cast<const char *>(&k_s);
        InsertExecutor<const char *, Stock> executor(stock_table,
                                                          k_stock,
                                                          k_s_sz,
                                                          t_s,
                                                     txn_ctx_rt_s,
                                                          version_store);
        auto res = executor.Execute();
        assert(res == true);
        auto ret_code = executor.GetResultCode();
        if (!ret_code.IsCASFailure() && !ret_code.IsRetryFailure()){
            s_retry_rowids.pop_front();
        }
    }
    auto res_rt_s = txn_manager->CommitTransaction(txn_ctx_rt_s);
    LOG_INFO("LoadOrderLine insert retry, current thread id :%d transaction has finished.", thread_id);
    assert(res_rt_s == ResultType::SUCCESS);

    txn_ctx_rt_c = nullptr;
    delete txn_ctx_rt_c;
    txn_ctx_rt_d = nullptr;
    delete txn_ctx_rt_d;
    txn_ctx_rt_h = nullptr;
    delete txn_ctx_rt_h;
    txn_ctx_rt_no = nullptr;
    delete txn_ctx_rt_no;
    txn_ctx_rt_o = nullptr;
    delete txn_ctx_rt_o;
    txn_ctx_rt_ol = nullptr;
    delete txn_ctx_rt_ol;
    txn_ctx_rt_s = nullptr;
    delete txn_ctx_rt_s;
    txn_ctx_rt_w = nullptr;
    delete txn_ctx_rt_w;


}//END Insert Retry

}

std::set<uint64_t> supp_keys;
std::set<uint64_t> reg_keys;
std::set<uint64_t> nat_keys;
std::set<uint64_t> i_keys;
std::set<uint64_t> w_keys;
std::set<District::DistrictKey> d_keys;
std::set<Customer::CustomerKey> c_keys;
//std::set<CustomerIndexedColumns> c_s_keys;
std::set<uint64_t> h_keys;
std::set<Stock::StockKey> s_keys;
std::set<Order::OrderKey> o_keys;
//std::set<OrderIndexedColumns> o_s_keys;
std::set<NewOrder::NewOrderKey> n_keys;
std::set<OrderLine::OrderLineKey> ol_keys;


void ScanLeafNode(BaseNode *real_root, std::string table_name) {
    auto leaf_node = reinterpret_cast<LeafNode *>(real_root) ;
    auto record_count = leaf_node->GetHeader()->GetStatus().GetRecordCount();
    for (uint32_t i = 0; i < record_count; ++i) {
        RecordMetadata meta = leaf_node->GetMetadata(i);
        char *payload = nullptr;
        char *key = nullptr;
        leaf_node->GetRawRecord(meta, &key, &payload);
        if(table_name == "w"){
            uint64_t key_ = *reinterpret_cast<const uint64_t *>(key);
            w_keys.insert(key_);
        }else if(table_name == "d"){
            District::DistrictKey key_ = *reinterpret_cast<const District::DistrictKey *>(key);
            d_keys.insert(key_);
        }else if(table_name == "c"){
            Customer::CustomerKey key_ = *reinterpret_cast<const Customer::CustomerKey *>(key);
            c_keys.insert(key_);
        }else if(table_name == "cs"){
//            CustomerIndexedColumns key_ = *reinterpret_cast<const CustomerIndexedColumns *>(key);
//            c_s_keys.insert(key_);
        }else if(table_name == "h"){
            uint64_t key_ = *reinterpret_cast<const uint64_t *>(key);
            h_keys.insert(key_);
        }else if(table_name == "s"){
            Stock::StockKey key_ = *reinterpret_cast<const Stock::StockKey *>(key);
            s_keys.insert(key_);
        }else if(table_name == "o"){
            Order::OrderKey key_ = *reinterpret_cast<const Order::OrderKey *>(key);
            o_keys.insert(key_);
        }else if(table_name == "os"){
//            OrderIndexedColumns key_ = *reinterpret_cast<const OrderIndexedColumns *>(key);
//            o_s_keys.insert(key_);
        }else if(table_name == "n"){
            NewOrder::NewOrderKey key_ = *reinterpret_cast<const NewOrder::NewOrderKey *>(key);
            n_keys.insert(key_);
        }else if(table_name == "ol"){
            OrderLine::OrderLineKey key_ = *reinterpret_cast<const OrderLine::OrderLineKey *>(key);
            ol_keys.insert(key_);
        }else if(table_name == "i"){
            uint64_t key_ = *reinterpret_cast<const uint64_t *>(key);
            i_keys.insert(key_);
        }else if(table_name == "nat"){
            uint64_t key_ = *reinterpret_cast<const uint64_t *>(key);
            nat_keys.insert(key_);
        }else if(table_name == "reg"){
            uint64_t key_ = *reinterpret_cast<const uint64_t *>(key);
            reg_keys.insert(key_);
        }else if(table_name == "supp"){
            uint64_t key_ = *reinterpret_cast<const uint64_t *>(key);
            supp_keys.insert(key_);
        }


    }
}
void ScanInnerNode(BaseNode *real_root,std::string table_name) {
    auto inner_node = reinterpret_cast<InternalNode *>(real_root);
    auto sorted_count = inner_node->GetHeader()->sorted_count;
    for (uint32_t i = 0; i < sorted_count; ++i) {
        RecordMetadata meta = inner_node->GetMetadata(i);

        char *ptr = reinterpret_cast<char *>(inner_node) + meta.GetOffset() + meta.GetPaddedKeyLength();
        uint64_t node_addr =  *reinterpret_cast<uint64_t *>(ptr);
        BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
        if (node->IsLeaf()) {
            ScanLeafNode(node,table_name);
        } else if (node == nullptr){
            break;
        }else {
            ScanInnerNode(node,table_name);
        }
    }
}
void ScanItemTables(BTree *i_table){
    std::string table_name = "i";
    i_keys.clear();
    auto real_root = i_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("item real count = %lu", i_keys.size());

    i_keys.clear();
}
void ScanWarehouseTables(BTree *w_table){
    std::string table_name = "w";
    w_keys.clear();
    auto real_root = w_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("warehouse real count = %lu", w_keys.size());

    w_keys.clear();
}
void ScanDistrictTables(BTree *d_table){
    std::string table_name = "d";
    d_keys.clear();
    auto real_root = d_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("district real count = %lu", d_keys.size());

    d_keys.clear();
}
void ScanCustomerTables(BTree *c_table){
    std::string table_name = "c";
    c_keys.clear();
    auto real_root = c_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("customer real count = %lu", c_keys.size());

    c_keys.clear();
}
void ScanCustomerSKeyTables(BTree *c_s_table){
//    std::string table_name = "cs";
//    c_s_keys.clear();
//    auto real_root = c_s_table->GetRootNodeSafe();
//
//    if (real_root->IsLeaf()){
//        ScanLeafNode(real_root,table_name);
//    }else{
//        ScanInnerNode(real_root,table_name);
//    }
//    LOG_INFO("customer skey real count = %lu", c_s_keys.size());
//
//    c_s_keys.clear();
}
void ScanHistoryTables(BTree *h_table){
    std::string table_name = "h";
    h_keys.clear();
    auto real_root = h_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("history real count = %lu", h_keys.size());

    h_keys.clear();
}
void ScanStockTables(BTree *s_table){
    std::string table_name = "s";
    s_keys.clear();
    auto real_root = s_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("stock real count = %lu", s_keys.size());

    s_keys.clear();
}
void ScanOrderTables(BTree *o_table){
    std::string table_name = "o";
    o_keys.clear();
    auto real_root = o_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("order real count = %lu", o_keys.size());

    o_keys.clear();
}
void ScanOrderSkeyTables(BTree *o_s_table){
//    std::string table_name = "os";
//    o_s_keys.clear();
//    auto real_root = o_s_table->GetRootNodeSafe();
//
//    if (real_root->IsLeaf()){
//        ScanLeafNode(real_root,table_name);
//    }else{
//        ScanInnerNode(real_root,table_name);
//    }
//    LOG_INFO("order skey real count = %lu", o_s_keys.size());
//
//    o_s_keys.clear();
}
void ScanNewOrderTables(BTree *n_table){
    std::string table_name = "n";
    n_keys.clear();
    auto real_root = n_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("new order real count = %lu", n_keys.size());

    n_keys.clear();
}
void ScanOrderLineTables(BTree *ol_table){
    std::string table_name = "ol";
    ol_keys.clear();
    auto real_root = ol_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("order line real count = %lu", ol_keys.size());

    ol_keys.clear();
}
void ScanNationTables(BTree *nat_table){
    std::string table_name = "nat";
    nat_keys.clear();
    auto real_root = nat_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("nation real count = %lu", nat_keys.size());

    nat_keys.clear();
}
void ScanRegionTables(BTree *reg_table){
    std::string table_name = "reg";
    reg_keys.clear();
    auto real_root = reg_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("region real count = %lu", reg_keys.size());

    reg_keys.clear();
}
void ScanSuppTables(BTree *supp_table){
    std::string table_name = "supp";
    supp_keys.clear();
    auto real_root = supp_table->GetRootNodeSafe();

    if (real_root->IsLeaf()){
        ScanLeafNode(real_root,table_name);
    }else{
        ScanInnerNode(real_root,table_name);
    }
    LOG_INFO("supplier real count = %lu", supp_keys.size());

    supp_keys.clear();
}
void LoadTPCCDatabase(VersionStore *version_store) {

    std::chrono::steady_clock::time_point start_time;
    start_time = std::chrono::steady_clock::now();
    int load_thread_num = state.loader_count;
    sync::ThreadPool the_tp(load_thread_num);

    LoadItems(version_store,0);
    LoadNation(version_store,0);
    LoadRegion(version_store,0);
    LoadSupplier(version_store,0);

    std::vector<std::unique_ptr<std::thread>> load_threads(load_thread_num);

    if (state.warehouse_count < load_thread_num) {
        for (int thread_id = 0; thread_id < state.warehouse_count; ++thread_id) {
            int warehouse_from = thread_id;
            int warehouse_to = thread_id + 1;

            load_threads[thread_id].reset(new std::thread(LoadWarehouses, version_store, thread_id,
                                                          warehouse_from, warehouse_to));
        }
    } else {
        int warehouse_per_thread = state.warehouse_count / load_thread_num;
        for (int thread_id = 0; thread_id < load_thread_num - 1; ++thread_id) {
            int warehouse_from = warehouse_per_thread * thread_id;
            int warehouse_to = warehouse_per_thread * (thread_id + 1);

            load_threads[thread_id].reset(new std::thread(LoadWarehouses, version_store, thread_id,
                                                          warehouse_from, warehouse_to));
        }
        int thread_id = load_thread_num - 1;
        int warehouse_from = warehouse_per_thread * thread_id;
        int warehouse_to = state.warehouse_count;

        load_threads[thread_id].reset(new std::thread(LoadWarehouses, version_store, thread_id,
                                                      warehouse_from, warehouse_to));
    }

    for (int thread_id = 0; thread_id < load_thread_num; ++thread_id) {
        load_threads[thread_id]->join();
    }


    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG_INFO("database loading time = %lf ms", diff);


    LOG_INFO("warehouse count = %d", state.warehouse_count);
    LOG_INFO("item count = %d", state.item_count);
    LOG_INFO("district count = %d", (state.warehouse_count * state.districts_per_warehouse));
    LOG_INFO("customer count = %d", (state.warehouse_count * state.districts_per_warehouse * state.customers_per_district));
    LOG_INFO("history count = %d", (state.warehouse_count * state.districts_per_warehouse * state.customers_per_district));
    LOG_INFO("orders count = %d", (state.warehouse_count * state.districts_per_warehouse * state.customers_per_district));
    LOG_INFO("stock count = %d", (state.warehouse_count * state.item_count));


    ScanItemTables(item_table);
    ScanWarehouseTables(warehouse_table);
    ScanDistrictTables(district_table);
    ScanCustomerTables(customer_table);
//    ScanCustomerSKeyTables(customer_skey_table);
    ScanHistoryTables(history_table);
    ScanStockTables(stock_table);
    ScanOrderTables(orders_table);
//    ScanOrderSkeyTables(orders_skey_table);
    ScanNewOrderTables(new_order_table);
    ScanOrderLineTables(order_line_table);
    ScanRegionTables(region_table);
    ScanNationTables(nation_table);
    ScanSuppTables(supplier_table);
}



void DestroyTPCCDatabase(VersionBlockManager *version_block_mng,
                         VersionStore *version_store,
                         SSNTransactionManager *txn_mng) {
    version_block_mng->ClearVersionBlock();
    version_store->CleanVersionStore();
    txn_mng->ClearInstance();

    delete item_table;
    item_table = nullptr;
    delete warehouse_table;
    warehouse_table = nullptr;
    delete district_table;
    district_table = nullptr;
    delete customer_table;
    customer_table = nullptr;
    delete history_table;
    history_table = nullptr;
    delete orders_table;
    orders_table = nullptr;
    delete new_order_table;
    new_order_table = nullptr;
    delete order_line_table;
    order_line_table = nullptr;
    delete stock_table;
    stock_table = nullptr;
    delete orders_skey_table;
    orders_skey_table = nullptr;
    delete customer_skey_table;
    customer_skey_table = nullptr;


    tpcc_catalogs.clear();
    database_tables.clear();
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

#ifdef __APPLE__
#pragma clang diagnostic pop
#endif