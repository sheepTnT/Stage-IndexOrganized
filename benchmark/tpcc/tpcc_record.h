//
// Created by zhangqian on 2022-02-27.
//

#ifndef MVSTORE_TPCC_RECORD_H
#define MVSTORE_TPCC_RECORD_H

#include <string>

#include "../../include/execute/txn.h"
#include "murmur/MurmurHash2.h"

namespace mvstore {
namespace benchmark {
namespace tpcc {




struct Warehouse {
//    int32_t W_ID;
    int64_t W_ID;
    char W_NAME[16];
    char W_STREET_1[32];
    char W_STREET_2[32];
    char W_CITY[32];
    char W_STATE[2];
    char W_ZIP[9];
    char W_TAX[8];//double
    char W_YTD[8];//double

    int64_t Key() const {
        return W_ID;
    }

    //GetData return the payload
    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint32_t) ;
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, W_NAME, sizeof(W_NAME));
        sz += sizeof(W_NAME);
        memcpy(payload_+sz, W_STREET_1, sizeof(W_STREET_1));
        sz += sizeof(W_STREET_1);
        memcpy(payload_+sz, W_STREET_2, sizeof(W_STREET_2));
        sz += sizeof(W_STREET_2);
        memcpy(payload_+sz, W_CITY, sizeof(W_CITY));
        sz += sizeof(W_CITY);
        memcpy(payload_+sz, W_STATE, sizeof(W_STATE));
        sz += sizeof(W_STATE);
        memcpy(payload_+sz, W_ZIP, sizeof(W_ZIP));
        sz += sizeof(W_ZIP);
        memcpy(payload_+sz, W_TAX, sizeof(W_TAX));
        sz += sizeof(W_TAX);
        memcpy(payload_+sz, W_YTD, sizeof(W_YTD));


        return payload_;
    }

    static uint32_t GetPayloadSize() {
        uint32_t payload_sz = sizeof(W_NAME) + sizeof(W_STREET_1)+ sizeof(W_STREET_2)+
                                sizeof(W_CITY)+ sizeof(W_STATE)
                               + sizeof(W_ZIP)+ sizeof(W_TAX)+ sizeof(W_YTD);
        return payload_sz;
    }

    static uint32_t GetColumnNum(){
        uint32_t col_num = 9;
        return col_num;
    }
};


struct District {
//    int32_t D_W_ID;
//    int32_t D_ID;
    int64_t D_W_ID;
    int64_t D_ID;
    int32_t D_NEXT_O_ID;
    char D_NAME[16];
    char D_STREET_1[32];
    char D_STREET_2[32];
    char D_CITY[32];
    char D_STATE[2];
    char D_ZIP[9];
    char D_TAX[8];//double
    char D_YTD[8];//double

    struct DistrictKey {
        int64_t D_W_ID;
        int64_t D_ID;

        bool operator<(const DistrictKey &rhs) const {
            return D_W_ID < rhs.D_W_ID || D_W_ID == rhs.D_W_ID && D_ID < rhs.D_ID;
        }

        bool operator==(const DistrictKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
        char *Key()  const{
            char *key_ = new char[sizeof(DistrictKey)];
            uint32_t sz = 0;
            memcpy(key_+sz, &D_W_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &D_ID, sizeof(uint64_t));
            return key_;
        }
    };

    DistrictKey Key() const {
        return {D_W_ID, D_ID};
    }

    //GetData return the payload
    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t) + sizeof(uint64_t);
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &D_NEXT_O_ID, sizeof(uint32_t));
        sz += sizeof(D_NEXT_O_ID);
        memcpy(payload_+sz, D_NAME, sizeof(D_NAME));
        sz += sizeof(D_NAME);
        memcpy(payload_+sz, D_STREET_1, sizeof(D_STREET_1));
        sz += sizeof(D_STREET_1);
        memcpy(payload_+sz, D_STREET_2, sizeof(D_STREET_2));
        sz += sizeof(D_STREET_2);
        memcpy(payload_+sz, D_CITY, sizeof(D_CITY));
        sz += sizeof(D_CITY);
        memcpy(payload_+sz, D_STATE, sizeof(D_STATE));
        sz += sizeof(D_STATE);
        memcpy(payload_+sz,  D_ZIP, sizeof(D_ZIP));
        sz += sizeof(D_ZIP);
        memcpy(payload_+sz, D_TAX, sizeof(D_TAX));
        sz += sizeof(D_TAX);
        memcpy(payload_+sz, D_YTD, sizeof(D_YTD));


//        auto v_org = *reinterpret_cast<const uint32_t *>(payload_+sz);
        return payload_;
    }

    static uint32_t GetPayloadSize() {
        uint32_t payload_sz = sizeof(D_NAME) + sizeof(D_STREET_1) +sizeof(D_STREET_2)+
                                sizeof(D_CITY) + sizeof(D_STATE)+sizeof(D_ZIP)+sizeof(D_TAX)+
                                sizeof(D_YTD) + sizeof(D_NEXT_O_ID);
        return payload_sz;
    }
    static uint32_t GetColumnNum(){
        uint32_t col_num = 11;
        return col_num;
    }
};

struct Item {
//    int32_t I_ID;
    int64_t I_ID;
    int32_t I_IM_ID;
    char I_NAME[32];
    char I_PRICE[8];//double
    char I_DATA[64];

    int64_t Key() const {
        return I_ID;
    }
    //GetData return the payload
    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t) ;
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &I_IM_ID, sizeof(I_IM_ID));
        sz += sizeof(I_IM_ID);
        memcpy(payload_+sz, I_NAME, sizeof(I_NAME));
        sz += sizeof(I_NAME);
        memcpy(payload_+sz, I_PRICE, sizeof(I_PRICE));
        sz += sizeof(I_PRICE);
        memcpy(payload_+sz, I_DATA, sizeof(I_DATA));

        return payload_;
    }

    static uint32_t GetPayloadSize() {
        uint32_t payload_sz = sizeof(I_IM_ID)+ sizeof(I_NAME)+ sizeof(I_PRICE)+sizeof(I_DATA);
        return payload_sz;
    }

};

/**
 * put the integer in the front,
 * because we will deserialize from the struct
 * directlty
 */
struct Customer {
//    int32_t C_W_ID;
//    int32_t C_D_ID;
//    int32_t C_ID;
    int64_t C_W_ID;
    int64_t C_D_ID;
    int64_t C_ID;
    uint64_t C_SINCE;
    int32_t C_PAYMENT_CNT;
    int32_t C_DELIVERY_CNT;
    char C_FIRST[32];
    char C_MIDDLE[2];
    char C_LAST[32];
    char C_STREET_1[32];
    char C_STREET_2[32];
    char C_CITY[32];
    char C_STATE[2];
    char C_ZIP[9];
    char C_PHONE[32];
    char C_CREDIT[2];
    char C_CREDIT_LIM[8];//double
    char C_DISCOUNT[8];//double
    char C_BALANCE[8];//double
    char C_YTD_PAYMENT[8];//double
    char C_DATA[500];

    struct CustomerKey {
        int64_t C_W_ID;
        int64_t C_D_ID;
        int64_t C_ID;

        bool operator<(const CustomerKey &rhs) const {
            return C_W_ID < rhs.C_W_ID || C_W_ID == rhs.C_W_ID && C_D_ID < rhs.C_D_ID ||
                   C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID && C_ID < rhs.C_ID;
        }

        bool operator==(const CustomerKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
        char *Key()  {
            char *key_ = new char[sizeof(CustomerKey)];
            uint32_t sz = 0;
            memcpy(key_+sz, &C_W_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &C_D_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &C_ID, sizeof(uint64_t));
            return key_;
        }
    };

    CustomerKey Key() const {
        return {C_W_ID, C_D_ID, C_ID};
    }

    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &C_SINCE, sizeof(C_SINCE));
        sz += sizeof(C_SINCE);
        memcpy(payload_+sz, &C_PAYMENT_CNT, sizeof(C_PAYMENT_CNT));
        sz += sizeof(C_PAYMENT_CNT);
        memcpy(payload_+sz, &C_DELIVERY_CNT, sizeof(C_DELIVERY_CNT));
        sz += sizeof(C_DELIVERY_CNT);
        memcpy(payload_+sz, C_FIRST, sizeof(C_FIRST));
        sz += sizeof(C_FIRST);
        memcpy(payload_+sz, C_MIDDLE, sizeof(C_MIDDLE));
        sz += sizeof(C_MIDDLE);
        memcpy(payload_+sz, C_LAST, sizeof(C_LAST));
        sz += sizeof(C_LAST);
        memcpy(payload_+sz, C_STREET_1, sizeof(C_STREET_1));
        sz += sizeof(C_STREET_1);
        memcpy(payload_+sz, C_STREET_2, sizeof(C_STREET_2));
        sz += sizeof(C_STREET_2);
        memcpy(payload_+sz, C_CITY, sizeof(C_CITY));
        sz += sizeof(C_CITY);
        memcpy(payload_+sz, C_STATE, sizeof(C_STATE));
        sz += sizeof(C_STATE);
        memcpy(payload_+sz, C_ZIP, sizeof(C_ZIP));
        sz += sizeof(C_ZIP);
        memcpy(payload_+sz, C_PHONE, sizeof(C_PHONE));
        sz += sizeof(C_PHONE);
        memcpy(payload_+sz, C_CREDIT, sizeof(C_CREDIT));
        sz += sizeof(C_CREDIT);
        memcpy(payload_+sz, C_CREDIT_LIM, sizeof(C_CREDIT_LIM));
        sz += sizeof(C_CREDIT_LIM);
        memcpy(payload_+sz, C_DISCOUNT, sizeof(C_DISCOUNT));
        sz += sizeof(C_DISCOUNT);
        memcpy(payload_+sz, C_BALANCE, sizeof(C_BALANCE));
        sz += sizeof(C_BALANCE);
        memcpy(payload_+sz, C_YTD_PAYMENT, sizeof(C_YTD_PAYMENT));
        sz += sizeof(C_YTD_PAYMENT);
        memcpy(payload_+sz, C_DATA, sizeof(C_DATA));


        return payload_;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz = sizeof(C_FIRST) + sizeof(C_MIDDLE) +sizeof(C_LAST)+sizeof(C_STREET_1)+
                sizeof(C_STREET_2)+sizeof(C_CITY)+sizeof(C_STATE)+sizeof(C_ZIP)+sizeof(C_PHONE)+
                sizeof(C_SINCE)+sizeof(C_CREDIT)+sizeof(C_CREDIT_LIM)+sizeof(C_DISCOUNT)+sizeof(C_BALANCE)+
                sizeof(C_YTD_PAYMENT)+sizeof(C_PAYMENT_CNT)+sizeof(C_DELIVERY_CNT)+sizeof(C_DATA);

        return payload_sz;
    }

    static uint32_t GetColumnNum(){
        uint32_t col_num = 21;
        return col_num;
    }
};


struct CustomerIndexedColumns {
//    int32_t C_ID;
//    int32_t C_W_ID;
//    int32_t C_D_ID;
    int64_t C_ID;
    int64_t C_W_ID;
    int64_t C_D_ID;
    char C_LAST[32];

    bool operator<(const CustomerIndexedColumns &rhs) const {
        return C_W_ID < rhs.C_W_ID || C_W_ID == rhs.C_W_ID && C_D_ID < rhs.C_D_ID ||
               C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID && std::string(C_LAST) < std::string(rhs.C_LAST) ||
               C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID && std::string(C_LAST) == std::string(rhs.C_LAST) &&
               C_ID < rhs.C_ID;
    }

    CustomerIndexedColumns Key() const {
        return *this;
    }
    char *Key()  {
        char *key_ = new char[sizeof(CustomerIndexedColumns)];
        uint32_t sz = 0;
        memcpy(key_+sz, &C_ID, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(key_+sz, &C_W_ID, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(key_+sz, &C_D_ID, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(key_+sz, C_LAST, 32);
        return key_;
    }

    bool operator==(const CustomerIndexedColumns &rhs) const {
        return C_ID == rhs.C_ID && C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID &&
               std::string(C_LAST) == std::string(rhs.C_LAST);
    }

    const char *GetData() const{
        return nullptr;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz =  0;
        return payload_sz;
    }
};


struct History {
//    int32_t H_ID;
    int64_t H_ID;
    int32_t H_C_ID;
    int32_t H_C_D_ID;
    int32_t H_C_W_ID;
    int32_t H_D_ID;
    int32_t H_W_ID;
    uint64_t H_DATE;
    char H_AMOUNT[8];//double
    char H_DATA[32];

    int64_t Key() const {
        return H_ID;
    }

    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t)  ;
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &H_C_ID, sizeof(H_C_ID));
        sz += sizeof(H_C_ID);
        memcpy(payload_+sz, &H_C_D_ID, sizeof(H_C_D_ID));
        sz += sizeof(H_C_D_ID);
        memcpy(payload_+sz, &H_C_W_ID, sizeof(H_C_W_ID));
        sz += sizeof(H_C_W_ID);
        memcpy(payload_+sz, &H_D_ID, sizeof(H_D_ID));
        sz += sizeof(H_D_ID);
        memcpy(payload_+sz, &H_W_ID, sizeof(H_W_ID));
        sz += sizeof(H_W_ID);
        memcpy(payload_+sz, &H_DATE, sizeof(H_DATE));
        sz += sizeof(H_DATE);
        memcpy(payload_+sz, H_AMOUNT, sizeof(H_AMOUNT));
        sz += sizeof(H_AMOUNT);
        memcpy(payload_+sz, H_DATA, sizeof(H_DATA));

        return payload_;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz = sizeof(H_C_ID) + sizeof(H_C_D_ID) + sizeof(H_C_W_ID)+
                              sizeof(H_D_ID)+
                              sizeof(H_W_ID)+ sizeof(H_DATE)+ sizeof(H_AMOUNT) +
                              sizeof(H_DATA);
        return payload_sz;
    }
};

struct Stock {
//    int32_t S_W_ID;
//    int32_t S_I_ID;
    int64_t S_W_ID;
    int64_t S_I_ID;
    int32_t S_QUANTITY;
    int32_t S_YTD;
    int32_t S_ORDER_CNT;
    int32_t S_REMOTE_CNT;
    char S_DIST_01[32];
    char S_DIST_02[32];
    char S_DIST_03[32];
    char S_DIST_04[32];
    char S_DIST_05[32];
    char S_DIST_06[32];
    char S_DIST_07[32];
    char S_DIST_08[32];
    char S_DIST_09[32];
    char S_DIST_10[32];
    char S_DATA[64];

    struct StockKey {
        int64_t S_W_ID;
        int64_t S_I_ID;

        bool operator<(const StockKey &rhs) const {
            return S_W_ID < rhs.S_W_ID || S_W_ID == rhs.S_W_ID && S_I_ID < rhs.S_I_ID;
        }

        bool operator==(const StockKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
        char *Key()  {
            char *key_ = new char[sizeof(StockKey)];
            uint32_t sz = 0;
            memcpy(key_+sz, &S_W_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &S_I_ID, sizeof(uint64_t));

            return key_;
        }
    };

    StockKey Key() const {
        return {S_W_ID, S_I_ID};
    }

    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t) + sizeof(uint64_t) ;
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &S_QUANTITY, sizeof(S_QUANTITY));
        sz += sizeof(S_QUANTITY);
        memcpy(payload_+sz, &S_YTD, sizeof(S_YTD));
        sz += sizeof(S_YTD);
        memcpy(payload_+sz, &S_ORDER_CNT, sizeof(S_ORDER_CNT));
        sz += sizeof(S_ORDER_CNT);
        memcpy(payload_+sz, &S_REMOTE_CNT, sizeof(S_REMOTE_CNT));
        sz += sizeof(S_REMOTE_CNT);
        memcpy(payload_+sz, S_DIST_01, sizeof(S_DIST_01));
        sz += sizeof(S_DIST_01);
        memcpy(payload_+sz, S_DIST_02, sizeof(S_DIST_02));
        sz += sizeof(S_DIST_02);
        memcpy(payload_+sz, S_DIST_03, sizeof(S_DIST_03));
        sz += sizeof(S_DIST_03);
        memcpy(payload_+sz, S_DIST_04, sizeof(S_DIST_04));
        sz += sizeof(S_DIST_04);
        memcpy(payload_+sz, S_DIST_05, sizeof(S_DIST_05));
        sz += sizeof(S_DIST_05);
        memcpy(payload_+sz, S_DIST_06, sizeof(S_DIST_06));
        sz += sizeof(S_DIST_06);
        memcpy(payload_+sz, S_DIST_07, sizeof(S_DIST_07));
        sz += sizeof(S_DIST_07);
        memcpy(payload_+sz, S_DIST_08, sizeof(S_DIST_08));
        sz += sizeof(S_DIST_08);
        memcpy(payload_+sz, S_DIST_09, sizeof(S_DIST_09));
        sz += sizeof(S_DIST_09);
        memcpy(payload_+sz, S_DIST_10, sizeof(S_DIST_10));
        sz += sizeof(S_DIST_10);
        memcpy(payload_+sz, S_DATA, sizeof(S_DATA));

        return payload_;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz =  sizeof(S_QUANTITY) + sizeof(S_DIST_01)+sizeof(S_DIST_02)+sizeof(S_DIST_03)+
                                sizeof(S_DIST_04)+sizeof(S_DIST_05)+sizeof(S_DIST_06)+sizeof(S_DIST_07)+sizeof(S_DIST_08)
                                +sizeof(S_DIST_09)+sizeof(S_DIST_10)+sizeof(S_YTD)+
                                sizeof(S_ORDER_CNT)+sizeof(S_REMOTE_CNT)+sizeof(S_DATA);
        return payload_sz;
    }

    static uint32_t GetColumnNum(){
        uint32_t col_num = 17;
        return col_num;
    }
};

struct Order {
//    int32_t O_W_ID;
//    int32_t O_D_ID;
//    int32_t O_ID;
    int64_t O_W_ID;
    int64_t O_D_ID;
    int64_t O_ID;
    int32_t O_C_ID;
    uint64_t O_ENTRY_D;
    int32_t O_CARRIER_ID;
    int32_t O_OL_CNT;
    int32_t O_ALL_LOCAL;

    struct OrderKey {
        int64_t O_W_ID;
        int64_t O_D_ID;
        int64_t O_ID;

        bool operator<(const OrderKey &rhs) const {
            return O_W_ID < rhs.O_W_ID || O_W_ID == rhs.O_W_ID && O_D_ID < rhs.O_D_ID ||
                   O_W_ID == rhs.O_W_ID && O_D_ID == rhs.O_D_ID && O_ID < rhs.O_ID;
        }

        bool operator==(const OrderKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
        char *Key()  {
            char *key_ = new char[sizeof(OrderKey)];
            uint32_t sz = 0;
            memcpy(key_+sz, &O_W_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &O_D_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &O_ID, sizeof(uint64_t));
            return key_;
        }
    };

    OrderKey Key() const {
        return {O_W_ID, O_D_ID, O_ID};
    }

    //GetData return the payload
    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t);
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &O_C_ID, sizeof(O_C_ID));
        sz += sizeof(O_C_ID);
        memcpy(payload_+sz, &O_ENTRY_D, sizeof(O_ENTRY_D));
        sz += sizeof(O_ENTRY_D);
        memcpy(payload_+sz, &O_CARRIER_ID, sizeof(O_CARRIER_ID));
        sz += sizeof(O_CARRIER_ID);
        memcpy(payload_+sz, &O_OL_CNT, sizeof(O_OL_CNT));
        sz += sizeof(O_OL_CNT);
        memcpy(payload_+sz, &O_ALL_LOCAL, sizeof(O_ALL_LOCAL));

        return payload_;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz = sizeof(O_C_ID) +  sizeof(O_ENTRY_D) +
                              sizeof(O_CARRIER_ID) + sizeof(O_OL_CNT)+ sizeof(O_ALL_LOCAL);
        return payload_sz;
    }

    static uint32_t GetColumnNum(){
        uint32_t col_num = 8;
        return col_num;
    }
};

struct OrderIndexedColumns {
    OrderIndexedColumns() {}

    OrderIndexedColumns(const Order &o) : O_ID(o.O_ID), O_C_ID(o.O_C_ID), O_D_ID(o.O_D_ID), O_W_ID(o.O_W_ID) {}

//    int32_t O_ID;
//    int32_t O_C_ID;
//    int32_t O_D_ID;
//    int32_t O_W_ID;
    int64_t O_ID;
    int64_t O_C_ID;
    int64_t O_D_ID;
    int64_t O_W_ID;

    bool operator<(const OrderIndexedColumns &rhs) const {
        return O_W_ID < rhs.O_W_ID || O_W_ID == rhs.O_W_ID && O_D_ID < rhs.O_D_ID ||
               O_W_ID == rhs.O_W_ID && O_D_ID == rhs.O_D_ID && O_C_ID < rhs.O_C_ID ||
               O_W_ID == rhs.O_W_ID && O_D_ID == rhs.O_D_ID && O_C_ID == rhs.O_C_ID && O_ID < rhs.O_ID;
    }

    OrderIndexedColumns Key() const {
        return *this;
    }
    char *Key()  {
        char *key_ = new char[sizeof(OrderIndexedColumns)];
        uint32_t sz = 0;
        memcpy(key_+sz, &O_ID, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(key_+sz, &O_C_ID, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(key_+sz, &O_D_ID, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(key_+sz, &O_W_ID, sizeof(uint64_t));
        return key_;
    }
    bool operator==(const OrderIndexedColumns &rhs) const {
        return !(*this < rhs) && !(rhs < *this);
    }

    //GetData return the payload
    const char *GetData() const{
        return nullptr;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz = 0;
        return payload_sz;
    }
};


struct NewOrder {
//    int32_t NO_W_ID;
//    int32_t NO_D_ID;
//    int32_t NO_O_ID;
    int64_t NO_W_ID;
    int64_t NO_D_ID;
    int64_t NO_O_ID;

    struct NewOrderKey {
        int64_t NO_W_ID;
        int64_t NO_D_ID;
        int64_t NO_O_ID;

        bool operator<(const NewOrderKey &rhs) const {
            return NO_D_ID < rhs.NO_D_ID || NO_D_ID == rhs.NO_D_ID && NO_W_ID < rhs.NO_W_ID ||
                   NO_D_ID == rhs.NO_D_ID && NO_W_ID == rhs.NO_W_ID && NO_O_ID < rhs.NO_O_ID;
        }

        bool operator==(const NewOrderKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs),
                          sizeof(NewOrderKey)) == 0;
        }
        char *Key()  {
            char *key_ = new char[sizeof(NewOrderKey)];
            uint32_t sz = 0;
            memcpy(key_+sz, &NO_W_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &NO_D_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &NO_O_ID, sizeof(uint64_t));
            return key_;
        }
    };

    NewOrderKey Key() const {
        return {NO_D_ID, NO_W_ID, NO_O_ID};
    }

    //GetData return the payload
    const char *GetData() const{
        return nullptr;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz = 0;
        return payload_sz;
    }
};


struct OrderLine {
//    int32_t OL_W_ID;
//    int32_t OL_D_ID;
//    int32_t OL_O_ID;
//    int32_t OL_NUMBER;
    int64_t OL_W_ID;
    int64_t OL_D_ID;
    int64_t OL_O_ID;
    int64_t OL_NUMBER;
    int32_t OL_I_ID;
    int32_t OL_SUPPLY_W_ID;
    int64_t OL_DELIVERY_D;
    int32_t OL_QUANTITY;
    char OL_AMOUNT[8];//double
    char OL_DIST_INFO[32];

    struct OrderLineKey {
        int64_t OL_W_ID;
        int64_t OL_D_ID;
        int64_t OL_O_ID;
        int64_t OL_NUMBER;

        bool operator<(const OrderLineKey &rhs) const {
            return OL_W_ID < rhs.OL_W_ID || OL_W_ID == rhs.OL_W_ID && OL_D_ID < rhs.OL_D_ID ||
                   OL_W_ID == rhs.OL_W_ID && OL_D_ID == rhs.OL_D_ID && OL_O_ID < rhs.OL_O_ID ||
                   OL_W_ID == rhs.OL_W_ID && OL_D_ID == rhs.OL_D_ID && OL_O_ID == rhs.OL_O_ID &&
                   OL_NUMBER < rhs.OL_NUMBER;
        }

        bool operator==(const OrderLineKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs),
                          sizeof(*this)) == 0;
        }
        char *Key()  {
            char *key_ = new char[sizeof(OrderLineKey)];
            uint32_t sz = 0;
            memcpy(key_+sz, &OL_W_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &OL_D_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &OL_O_ID, sizeof(uint64_t));
            sz += sizeof(uint64_t);
            memcpy(key_+sz, &OL_NUMBER, sizeof(uint64_t));
            return key_;
        }
    };

    OrderLineKey Key() const {
        return {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER};
    }

    //GetData return the payload
    const char *GetData() const{
//        auto data_ = reinterpret_cast<const char *>(this);
//        auto payload_ = data_ + sizeof(uint64_t) + sizeof(uint64_t)+ sizeof(uint64_t)+ sizeof(uint64_t);
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &OL_I_ID, sizeof(OL_I_ID));
        sz += sizeof(OL_I_ID);
        memcpy(payload_+sz, &OL_SUPPLY_W_ID, sizeof(OL_SUPPLY_W_ID));
        sz += sizeof(OL_SUPPLY_W_ID);
        memcpy(payload_+sz, &OL_DELIVERY_D, sizeof(OL_DELIVERY_D));
        sz += sizeof(OL_DELIVERY_D);
        memcpy(payload_+sz, &OL_QUANTITY, sizeof(OL_QUANTITY));
        sz += sizeof(OL_QUANTITY);
        memcpy(payload_+sz, OL_AMOUNT, sizeof(OL_AMOUNT));
        sz += sizeof(OL_AMOUNT);
        memcpy(payload_+sz, OL_DIST_INFO, sizeof(OL_DIST_INFO));

        return payload_;
    }

    static uint32_t GetPayloadSize()  {
        uint32_t payload_sz = sizeof(OL_I_ID) +  sizeof(OL_SUPPLY_W_ID) +sizeof(OL_DELIVERY_D) +
                              sizeof(OL_QUANTITY) + sizeof(OL_AMOUNT)+ sizeof(OL_DIST_INFO);
        return payload_sz;
    }
};
struct Region {
    int64_t R_REGIONKEY;
    char R_NAME[55];
    char R_COMMENT[152];

    int64_t Key() const {
        return R_REGIONKEY;
    }
    //GetData return the payload
    const char *GetData() const{
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, R_NAME, 55);
        sz += sizeof(R_NAME);
        memcpy(payload_+sz, R_COMMENT, 152);

        return payload_;
    }

    static uint32_t GetPayloadSize() {
        uint32_t payload_sz = 55+ 152;
        return payload_sz;
    }

};
struct Nation {
    int64_t N_NATIONKEY;
    int64_t N_REGIONKEY;
    char N_NAME[25];
    char N_COMMENT[152];

    int64_t Key() const {
        return N_NATIONKEY;
    }
    //GetData return the payload
    const char *GetData() const{
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &N_REGIONKEY, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(payload_+sz, N_NAME, 25);
        sz += sizeof(N_NAME);
        memcpy(payload_+sz, N_COMMENT, 152);

        return payload_;
    }

    static uint32_t GetPayloadSize() {
        uint32_t payload_sz =  sizeof(uint64_t)+ 25+152;
        return payload_sz;
    }

};
struct Supplier {
    int64_t SU_SUPPKEY;
    int64_t SU_NATIONKEY;
    char SU_ACCTBAL[8]; //double
    char SU_NAME[25];
    char SU_ADDRESS[40];
    char SU_PHONE[15];
    char SU_COMMENT[15];

    int64_t Key() const {
        return SU_SUPPKEY;
    }
    //GetData return the payload
    const char *GetData() const{
        uint32_t payload_sz = GetPayloadSize();
        char *payload_ = new char[payload_sz];
        uint32_t sz = 0;
        memcpy(payload_+sz, &SU_NATIONKEY, sizeof(uint64_t));
        sz += sizeof(uint64_t);
        memcpy(payload_+sz, SU_ACCTBAL, 8);
        sz += sizeof(SU_ACCTBAL);
        memcpy(payload_+sz, SU_NAME, 25);
        sz += sizeof(SU_NAME);
        memcpy(payload_+sz, SU_ADDRESS, 40);
        sz += sizeof(SU_ADDRESS);
        memcpy(payload_+sz, SU_PHONE, 15);
        sz += sizeof(SU_PHONE);
        memcpy(payload_+sz, SU_COMMENT, 15);

        return payload_;
    }

    static uint32_t GetPayloadSize() {
        uint32_t payload_sz = sizeof(uint64_t)+ 8 + 25 + 40 + 15 + 15;
        return payload_sz;
    }

};


const Nation nations[] = {{48, 0, "ALGERIA"},
                          {49, 1, "ARGENTINA"},
                          {50, 1, "BRAZIL"},
                          {51, 1, "CANADA"},
                          {52, 4, "EGYPT" },
                          {53, 0, "ETHIOPIA"},
                          {54, 3, "FRANCE"  },
                          {55, 3, "GERMANY" },
                          {56, 2, "INDIA"   },
                          {57, 2, "INDONESIA"},
                          {65, 4, "IRAN"     },
                          {66, 4, "IRAQ"     },
                          {67, 2, "JAPAN"    },
                          {68, 4, "JORDAN"   },
                          {69, 0, "KENYA"    },
                          {70, 0, "MOROCCO"  },
                          {71, 0, "MOZAMBIQUE"},
                          {72, 1, "PERU"      },
                          {73, 2, "CHINA"     },
                          {74, 3, "ROMANIA"   },
                          {75, 4, "SAUDI ARABIA"                                },
                          {76, 2, "VIETNAM"                                     },
                          {77, 3, "RUSSIA"                                      },
                          {78, 3, "UNITED KINGDOM"                              },
                          {79, 1, "UNITED STATES"                               },
                          {80, 2, "CHINA"                                       },
                          {81, 2, "PAKISTAN"                                    },
                          {82, 2, "BANGLADESH"                                  },
                          {83, 1, "MEXICO"                                      },
                          {84, 2, "PHILIPPINES"                                 },
                          {85, 2, "THAILAND"                                    },
                          {86, 3, "ITALY"                                       },
                          {87, 0, "SOUTH AFRICA"                                },
                          {88, 2, "SOUTH KOREA"                                 },
                          {89, 1, "COLOMBIA"                                    },
                          {90, 3, "SPAIN"                                       },
                          {97, 3, "UKRAINE"                                     },
                          {98, 3, "POLAND"                                      },
                          {99, 0, "SUDAN"                                       },
                          {100, 2, "UZBEKISTAN"                                 },
                          {101, 2, "MALAYSIA"                                   },
                          {102, 1, "VENEZUELA"                                  },
                          {103, 2, "NEPAL"                                      },
                          {104, 2, "AFGHANISTAN"                                },
                          {105, 2, "NORTH KOREA"                                },
                          {106, 2, "TAIWAN"                                     },
                          {107, 0, "GHANA"                                      },
                          {108, 0, "IVORY COAST"                                },
                          {109, 4, "SYRIA"                                      },
                          {110, 0, "MADAGASCAR"                                 },
                          {111, 0, "CAMEROON"                                   },
                          {112, 2, "SRI LANKA"                                  },
                          {113, 3, "ROMANIA"                                    },
                          {114, 3, "NETHERLANDS"                                },
                          {115, 2, "CAMBODIA"                                   },
                          {116, 3, "BELGIUM"                                    },
                          {117, 3, "GREECE"                                     },
                          {118, 3, "PORTUGAL"                                   },
                          {119, 4, "ISRAEL"                                     },
                          {120, 3, "FINLAND"                                    },
                          {121, 2, "SINGAPORE"                                  },
                          {122, 3, "NORWAY"                                     }};

static const char *regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

//typedef std::vector<std::vector<std::pair<int32_t, int32_t>>> SuppStockMap;

//static std::vector<std::vector<std::pair<int32_t, int32_t>>> supp_stock_map(10000);

}
}
}


namespace std {

template<>
class hash<mvstore::benchmark::tpcc::District::DistrictKey> {
public:
    size_t operator()(mvstore::benchmark::tpcc::District::DistrictKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<mvstore::benchmark::tpcc::Customer::CustomerKey> {
public:
    size_t operator()(mvstore::benchmark::tpcc::Customer::CustomerKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};


template<>
class hash<mvstore::benchmark::tpcc::CustomerIndexedColumns> {
public:
    size_t operator()(mvstore::benchmark::tpcc::CustomerIndexedColumns const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        size_t h1 = MurmurHash64A(data, sizeof(c) - sizeof(c.C_LAST), 0);
        return h1 ^ std::hash<std::string>()(std::string(c.C_LAST));
    }
};

template<>
class hash<mvstore::benchmark::tpcc::Stock::StockKey> {
public:
    size_t operator()(mvstore::benchmark::tpcc::Stock::StockKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<mvstore::benchmark::tpcc::Order::OrderKey> {
public:
    size_t operator()(mvstore::benchmark::tpcc::Order::OrderKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<mvstore::benchmark::tpcc::OrderIndexedColumns> {
public:
    size_t operator()(mvstore::benchmark::tpcc::OrderIndexedColumns const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<mvstore::benchmark::tpcc::NewOrder::NewOrderKey> {
public:
    size_t operator()(mvstore::benchmark::tpcc::NewOrder::NewOrderKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<mvstore::benchmark::tpcc::OrderLine::OrderLineKey> {
public:
    size_t operator()(mvstore::benchmark::tpcc::OrderLine::OrderLineKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};


}
#endif
