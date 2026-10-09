#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <base/unit.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int KEEPER_EXCEPTION;
}

namespace
{

/// Data of the root node. Bump on any layout change so old data is refused, not misread.
constexpr auto FORMAT_MARKER = "IcebergRESTCatalog\nformat_version: 1";

constexpr size_t MAX_UPDATE_RETRIES = 10;

String propertiesToJSON(const std::map<String, String> & properties)
{
    Poco::JSON::Object json;
    for (const auto & [key, value] : properties)
        json.set(key, value);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

/// Keeper limits node data size. The handler maps `BAD_ARGUMENTS` to a 400 response.
constexpr size_t MAX_NAMESPACE_PROPERTIES_SIZE = 64_KiB;

void checkPropertiesSize(const std::map<String, String> & properties)
{
    size_t size = 0;
    for (const auto & [key, value] : properties)
        size += key.size() + value.size();
    if (size > MAX_NAMESPACE_PROPERTIES_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'properties' must be at most {} bytes in total", MAX_NAMESPACE_PROPERTIES_SIZE);
}

std::map<String, String> propertiesFromJSON(const String & data, const String & path)
{
    Poco::JSON::Object::Ptr json;
    try
    {
        json = Poco::JSON::Parser().parse(data).extract<Poco::JSON::Object::Ptr>();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Namespace properties at {} are not a JSON object: {}", path, e.displayText());
    }

    std::map<String, String> properties;
    for (const auto & [key, value] : *json)
        properties[key] = value.extract<String>();
    return properties;
}

}

KeeperIcebergRESTCatalogStore::KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_)
    : root_path(std::move(root_path_))
    , get_zookeeper(std::move(get_zookeeper_))
{
}

zkutil::ZooKeeperPtr KeeperIcebergRESTCatalogStore::getZooKeeper() const
{
    /// Cheap on the hot path. Only a new session pays for `sync` and `initRoot`.
    std::lock_guard lock(mutex);
    auto zookeeper = get_zookeeper();
    if (zookeeper != initialized_zookeeper)
    {
        /// A new session may talk to another replica. Avoid reading stale state.
        zookeeper->sync(root_path);
        initRoot(zookeeper);
        /// Remember the session only after it passed the check, so a failed check is repeated.
        initialized_zookeeper = zookeeper;
    }
    return zookeeper;
}

void KeeperIcebergRESTCatalogStore::initRoot(const zkutil::ZooKeeperPtr & zookeeper) const
{
    zookeeper->createAncestors(root_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(root_path, FORMAT_MARKER, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root_path + "/namespaces", "", zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        zkutil::KeeperMultiException::check(code, ops, responses);

    const auto marker = zookeeper->get(root_path);
    if (marker != FORMAT_MARKER)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Iceberg REST catalog data at {} has an unsupported format: expected {}, found {}",
            root_path,
            FORMAT_MARKER,
            marker);
}

String KeeperIcebergRESTCatalogStore::namespacePath(const IcebergNamespaceName & name) const
{
    String path = root_path;
    for (const auto & level : name)
        path += "/namespaces/" + escapeForFileName(level);
    return path;
}

String KeeperIcebergRESTCatalogStore::childNamespacesPath(const IcebergNamespaceName & name) const
{
    return namespacePath(name) + "/namespaces";
}

bool KeeperIcebergRESTCatalogStore::createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createNamespace");
    checkPropertiesSize(properties);
    auto zookeeper = getZooKeeper();

    /// Each namespace is three nodes. Create them atomically so a crash cannot leave a partial namespace.
    const auto try_create = [&](const String & path, const std::map<String, String> & props)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(path, propertiesToJSON(props), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(path + "/namespaces", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(path + "/tables", "", zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        const auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
            return false;

        zkutil::KeeperMultiException::check(code, ops, responses);
        return true;
    };

    /// Keep the tree walkable from the root.
    for (size_t level = 1; level < name.size(); ++level)
    {
        const auto parent_path = namespacePath(IcebergNamespaceName(name.begin(), name.begin() + level));
        if (!zookeeper->exists(parent_path))
            try_create(parent_path, {});
    }

    return try_create(namespacePath(name), properties);
}

bool KeeperIcebergRESTCatalogStore::namespaceExists(const IcebergNamespaceName & name) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::namespaceExists");
    return getZooKeeper()->exists(namespacePath(name));
}

std::optional<UpdateNamespacePropertiesResult> KeeperIcebergRESTCatalogStore::updateNamespaceProperties(
    const IcebergNamespaceName & name, const std::map<String, String> & updates, const std::set<String> & removals)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::updateNamespaceProperties");
    auto zookeeper = getZooKeeper();
    const auto path = namespacePath(name);

    /// Read-modify-write with a version check, so two concurrent updates never lose a key.
    for (size_t attempt = 0; attempt < MAX_UPDATE_RETRIES; ++attempt)
    {
        Coordination::Stat stat;
        String data;
        if (!zookeeper->tryGet(path, data, &stat))
            return std::nullopt;

        auto properties = propertiesFromJSON(data, path);
        UpdateNamespacePropertiesResult result;
        for (const auto & key : removals)
        {
            if (properties.erase(key) > 0)
                result.removed.push_back(key);
            else
                result.missing.push_back(key);
        }
        for (const auto & [key, value] : updates)
        {
            properties[key] = value;
            result.updated.push_back(key);
        }

        checkPropertiesSize(properties);

        const auto code = zookeeper->trySet(path, propertiesToJSON(properties), stat.version);
        if (code == Coordination::Error::ZOK)
            return result;
        if (code == Coordination::Error::ZNONODE)
            return std::nullopt;
        if (code != Coordination::Error::ZBADVERSION)
            throw zkutil::KeeperException::fromPath(code, path);
    }

    throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Cannot update properties of namespace at {}: too many concurrent updates", path);
}

std::vector<IcebergNamespaceName> KeeperIcebergRESTCatalogStore::listNamespaces(const IcebergNamespaceName & parent) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::listNamespaces");
    Strings children;
    const auto code = getZooKeeper()->tryGetChildren(childNamespacesPath(parent), children);
    if (code == Coordination::Error::ZNONODE)
        return {};
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, childNamespacesPath(parent));

    /// Keeper returns children in an unspecified order.
    std::sort(children.begin(), children.end());

    std::vector<IcebergNamespaceName> result;
    result.reserve(children.size());
    for (const auto & child : children)
    {
        auto name = parent;
        name.push_back(unescapeForFileName(child));
        result.push_back(std::move(name));
    }
    return result;
}

}
