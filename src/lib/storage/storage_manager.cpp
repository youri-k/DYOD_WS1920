#include "storage_manager.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _storage_manager;

  return _storage_manager;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  DebugAssert(!has_table(name), "Another table with the same name already exists.");
  _tables.insert(std::make_pair(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  auto it = _tables.find(name);

  if (it == _tables.end()) throw std::invalid_argument("Invalid table name");

  _tables.erase(it);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  auto it = _tables.find(name);

  if (it == _tables.end()) throw std::invalid_argument("Invalid table name");

  // second is the actual shared_ptr to the table
  return it->second;
}

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) != 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (auto const& table_entry : _tables) {
    // first is the name of the table
    table_names.push_back(table_entry.first);
  }

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
