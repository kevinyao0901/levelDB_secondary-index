#include "leveldb/db.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <cassert>

using namespace std;
using namespace leveldb;

// 定义字段类型别名
using Field = std::pair<std::string, std::string>;
using FieldArray = std::vector<Field>;

// 二进制序列化函数
std::string SerializeValue(const FieldArray& fields) {
    std::ostringstream oss;
    uint32_t num_fields = fields.size();
    
    // 写入字段数量
    oss.write(reinterpret_cast<const char*>(&num_fields), sizeof(num_fields));

    // 写入每个字段的长度和内容
    for (const auto& field : fields) {
        uint32_t name_length = field.first.size();
        uint32_t value_length = field.second.size();
        
        // 写入字段名长度和字段名
        oss.write(reinterpret_cast<const char*>(&name_length), sizeof(name_length));
        oss.write(field.first.data(), name_length);

        // 写入字段值长度和字段值
        oss.write(reinterpret_cast<const char*>(&value_length), sizeof(value_length));
        oss.write(field.second.data(), value_length);
    }

    return oss.str();
}

// 二进制反序列化函数
FieldArray ParseValue(const std::string& value_str) {
    FieldArray fields;
    std::istringstream iss(value_str);

    uint32_t num_fields = 0;
    iss.read(reinterpret_cast<char*>(&num_fields), sizeof(num_fields)); // 读取字段数量

    for (uint32_t i = 0; i < num_fields; ++i) {
        uint32_t name_length = 0, value_length = 0;
        
        // 读取字段名长度和字段名
        iss.read(reinterpret_cast<char*>(&name_length), sizeof(name_length));
        std::string field_name(name_length, '\0');
        iss.read(&field_name[0], name_length);

        // 读取字段值长度和字段值
        iss.read(reinterpret_cast<char*>(&value_length), sizeof(value_length));
        std::string field_value(value_length, '\0');
        iss.read(&field_value[0], value_length);

        fields.emplace_back(field_name, field_value);
    }

    return fields;
}

// 查询函数：根据字段查找所有包含该字段的 Key
std::vector<std::string> FindKeysByField(leveldb::DB* db, const Field& field) {
    std::vector<std::string> keys;
    Iterator* it = db->NewIterator(ReadOptions());

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();

        FieldArray fields = ParseValue(value);

        // 查找是否有匹配的字段
        for (const auto& f : fields) {
            if (f.first == field.first && f.second == field.second) {
                keys.push_back(key);
                break;
            }
        }
    }

    delete it;
    return keys;
}

int main() {
    // 创建 LevelDB 数据库
    DB* db = nullptr;
    Options op;
    op.create_if_missing = true;
    Status status = DB::Open(op, "testdb", &db);
    assert(status.ok());

    // 插入两条数据，包含多个字段
    FieldArray fields1 = {
        {"name", "Customer#000000001"},
        {"address", "IVhzIApeRb"},
        {"phone", "25-989-741-2988"}
    };
    db->Put(WriteOptions(), "k_1", SerializeValue(fields1));

    FieldArray fields2 = {
        {"name", "Customer#000000002"},
        {"address", "N3qjPOETGc"},
        {"phone", "12-345-678-9012"}
    };
    db->Put(WriteOptions(), "k_2", SerializeValue(fields2));

    // 插入带有特殊字符的字段
    FieldArray fields3 = {
        {"name", "Customer=000000003"},
        {"address", "N3qj;POETGc"},
        {"phone", "12-345-678-9012"}
    };
    db->Put(WriteOptions(), "k_3", SerializeValue(fields3));

    // 读取并反序列化数据
    string value_ret1, value_ret2, value_ret3;
    db->Get(ReadOptions(), "k_1", &value_ret1);
    db->Get(ReadOptions(), "k_2", &value_ret2);
    db->Get(ReadOptions(), "k_3", &value_ret3);

    cout << "Deserialized fields for key k_1:" << endl;
    for (const auto& field : ParseValue(value_ret1)) {
        cout << field.first << ": " << field.second << endl;
    }

    cout << "\nDeserialized fields for key k_2:" << endl;
    for (const auto& field : ParseValue(value_ret2)) {
        cout << field.first << ": " << field.second << endl;
    }

    cout << "\nDeserialized fields for key k_3:" << endl;
    for (const auto& field : ParseValue(value_ret3)) {
        cout << field.first << ": " << field.second << endl;
    }

    // 查询功能测试：查找所有 name = "Customer#000000001" 的 key
    Field search_field = {"name", "Customer#000000001"};
    auto keys = FindKeysByField(db, search_field);

    // 输出查询结果
    cout << "\nFound keys for field " << search_field.first << " = " << search_field.second << ":" << endl;
    for (const auto& key : keys) {
        cout << key << endl;
    }

    delete db;
    return 0;
}
