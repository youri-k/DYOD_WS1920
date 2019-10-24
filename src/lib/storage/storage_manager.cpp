#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _storage_manager;

  return _storage_manager;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  _tables.insert(std::make_pair(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  DebugAssert(has_table(name), "The table doesn't contain the requested table");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) != 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  for (auto const& table : _tables) {
    table_names.push_back(table.first);
  }
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto const& table : _tables) {
    out << table.first << ", ";
    out << table.second->column_count() << ", ";
    out << table.second->row_count() << ", ";
    out << table.second->chunk_count() << std::endl;
  }
}

void StorageManager::reset() { _tables = std::unordered_map<std::string, std::shared_ptr<Table>>(); }

}  // namespace opossum
