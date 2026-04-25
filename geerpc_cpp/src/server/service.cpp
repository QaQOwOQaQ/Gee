#include "geerpc/server/service.h"
#include <stdexcept>

namespace geerpc {

Service::Service(std::string name) : name_(std::move(name)) {}

void Service::RegisterMethod(const std::string& method_name, HandlerFunc handler) {
    methods_.emplace(method_name, MethodInfo(std::move(handler)));
}

MethodInfo* Service::FindMethod(const std::string& method_name) {
    auto it = methods_.find(method_name);
    if (it == methods_.end()) return nullptr;
    return &it->second;
}

} // namespace geerpc
