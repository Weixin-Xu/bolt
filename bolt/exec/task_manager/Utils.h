#pragma once

#include <memory>

#include "bolt/connectors/Connector.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Split.h"

namespace bytedance::bolt {

exec::Split makeSplit(
    const std::string& connectorName,
    const std::string& filePath,
    dwio::common::FileFormat format,
    size_t offset = 0,
    size_t length = 0);

} // namespace bytedance::bolt
