//
// Created by zhangqian on 2021/11/12.
//

#include "../include/common/catalog.h"
#include <iostream>
#include <assert.h>

namespace mvstore{

void Catalog::init(const char * table_name_, int field_cnt_,
                   uint8_t *key_column_bitmap_, uint32_t key_length_,
                   uint32_t table_id_, bool is_log_) {
    this->table_name = table_name_;
    this->field_cnt = 0;
    this->_columns = new Column [field_cnt_];
    this->tuple_size = 0;
    this->key_bitmap = key_column_bitmap_;
    this->key_total_length = key_length_;
    this->table_id = table_id_;
    this->is_log = is_log_;
}

void Catalog::add_col(const char * col_name, uint32_t size, const char * type) {
    _columns[field_cnt].size = size;
    strcpy(_columns[field_cnt].type, type);
    strcpy(_columns[field_cnt].name, col_name);
    _columns[field_cnt].id = field_cnt;
    _columns[field_cnt].index = tuple_size;
    tuple_size += size;
    field_cnt ++;
}

uint32_t Catalog::get_field_id(const char * name) {
    uint32_t i;
    for (i = 0; i < field_cnt; i++) {
        if (strcmp(name, _columns[i].name) == 0)
            break;
    }
    assert (i < field_cnt);
    return i;
}

char *Catalog::get_field_type(uint32_t id) {
    return _columns[id].type;
}

char *Catalog::get_field_name(uint32_t id) {
    return _columns[id].name;
}


char *Catalog::get_field_type(char * name) {
    return get_field_type( get_field_id(name) );
}

uint32_t Catalog::get_field_index(char * name) {
    return get_field_index( get_field_id(name) );
}

void Catalog::print_schema() {
    printf("\n[Catalog] %s\n", table_name);
    for (uint32_t i = 0; i < field_cnt; i++) {
        std::cout << get_field_name(i) << "\t" << get_field_type(i)
                  << "\t" << get_field_size(i) << std::endl;
        //printf("\t%s\t%s\t%ld\n", get_field_name(i),
        //	get_field_type(i), get_field_size(i));
    }
}

}

