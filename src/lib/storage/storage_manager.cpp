#include "storage_manager.hpp"

#include <algorithm>
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
  DebugAssert(has_table(name), "The storage manager doesn't know the requested table");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) != 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (auto const& [name, _] : _tables) {
    table_names.push_back(name);
  }

  std::sort(table_names.begin(), table_names.end());

  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto const& [name, table] : _tables) {
    out << name << ", ";
    out << table->column_count() << ", ";
    out << table->row_count() << ", ";
    out << table->chunk_count() << std::endl;
  }
}

void StorageManager::reset() { _tables.clear(); }

}  // namespace opossum
