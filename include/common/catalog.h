#pragma once 

#include <map>
#include <vector>
#include <string.h>
//#include "constants.h"


namespace mvstore {

#define CL_SIZE	64

class Column {
public:
    Column() {
        this->type = new char[64];
        this->name = new char[64];
    }

    Column(uint32_t size, char *type, char *name,
           uint32_t id, uint32_t index) {
        this->size = size;
        this->id = id;
        this->index = index;
        this->type = new char[64];
        this->name = new char[64];
        strcpy(this->type, type);
        strcpy(this->name, name);
    };

    uint32_t id;
    uint32_t size;
    uint32_t index;
    char *type;
    char *name;
    char pad[CL_SIZE - sizeof(uint32_t) * 3 - sizeof(char *) * 2];
};

class Catalog {
public:
    // abandoned init function
    // field_size is the size of each each field.
    void init(const char *table_name, int field_cnt,
              uint8_t *key_column_bitmap, uint32_t key_length,
              uint32_t table_id, bool is_log);

    void add_col(const char *col_name, uint32_t size, const char *type);

    uint32_t get_tuple_size() const { return tuple_size; };

    uint16_t get_field_cnt() { return field_cnt; };

    uint32_t get_field_size(uint32_t id) const { return _columns[id].size; };

    uint32_t get_field_index(uint32_t id) const { return _columns[id].index; };

    char *get_field_type(uint32_t id);

    char *get_field_name(uint32_t id);

    uint32_t get_field_id(const char * name);

    char *get_field_type(char * name);

    uint32_t get_field_index(char * name);

    void print_schema();

    uint32_t get_key_length() const {return key_total_length;}

    uint8_t *get_key_columns() {return key_bitmap;}

    Column *_columns;
    uint32_t tuple_size;
    uint16_t field_cnt;
    const char *table_name;
    uint32_t table_id;
    uint8_t *key_bitmap;
    uint32_t key_total_length;
    bool is_log;
};
}
