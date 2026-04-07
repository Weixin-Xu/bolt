#include "bolt/exec/task_manager/Utils.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"

namespace bytedance::bolt {
exec::Split makeSplit(
    const std::string& connectorName,
    const std::string& filePath,
    dwio::common::FileFormat format,
    size_t offset,
    size_t length) {
  static const std::string kHiveConnectorId = "test-hive";
  if (connectorName == kHiveConnectorId) {
    auto hiveSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        connectorName, filePath, format, offset, length);
    return exec::Split(hiveSplit);
  }

  BOLT_UNREACHABLE(
      "Unsupported connector: {}. Please register the connector factory first.",
      connectorName);
}
} // namespace bytedance::bolt
