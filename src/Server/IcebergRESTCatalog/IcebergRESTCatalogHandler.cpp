#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

#include <Access/Credentials.h>
#include <Core/Settings.h>
#include <IO/HTTPCommon.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTPHandler.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <base/unit.h>
#include <boost/algorithm/string/split.hpp>
#include <fmt/ranges.h>

#include <algorithm>
#include <set>
#include <sstream>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_ddl;
    extern const SettingsUInt64 readonly;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int QUERY_IS_PROHIBITED;
    extern const int READONLY;
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ZOOKEEPER;
    extern const int REQUIRED_PASSWORD;
}

namespace
{

constexpr size_t MAX_REQUEST_BODY_SIZE = 1_MiB;
/// Keeper limits path depth. Reject oversized requests here with 400 instead of a Keeper error.
constexpr size_t MAX_NAMESPACE_LEVELS = 16;
constexpr size_t MAX_NAMESPACE_LEVEL_LENGTH = 256;
constexpr char NAMESPACE_LEVEL_SEPARATOR = '\x1F';

/// The handler maps `BAD_ARGUMENTS` to a 400 response.
void validateNamespace(const IcebergNamespaceName & name)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace must have at least one level");
    if (name.size() > MAX_NAMESPACE_LEVELS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace must have at most {} levels", MAX_NAMESPACE_LEVELS);
    for (const auto & level : name)
    {
        if (level.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must be non-empty");
        if (level.size() > MAX_NAMESPACE_LEVEL_LENGTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must be at most {} bytes", MAX_NAMESPACE_LEVEL_LENGTH);
    }
}

IcebergNamespaceName splitNamespace(const String & value)
{
    IcebergNamespaceName result;
    boost::split(result, value, [](char c) { return c == NAMESPACE_LEVEL_SEPARATOR; });
    validateNamespace(result);
    return result;
}

String joinNamespace(const IcebergNamespaceName & name)
{
    return fmt::format("{}", fmt::join(name, "."));
}

Poco::JSON::Array stringsToJSONArray(const Strings & strings)
{
    Poco::JSON::Array result;
    for (const auto & value : strings)
        result.add(value);
    return result;
}

std::optional<String> getQueryParameter(const Poco::URI & uri, const String & name)
{
    for (const auto & [key, value] : uri.getQueryParameters())
    {
        if (key == name)
            return value;
    }
    return std::nullopt;
}

}

IcebergRESTCatalogHandler::IcebergRESTCatalogHandler(IServer & server_, IcebergRESTCatalogWarehousesPtr warehouses_)
    : log(getLogger("IcebergRESTCatalogHandler"))
    , server(server_)
    , warehouses(std::move(warehouses_))
{
}

ContextMutablePtr IcebergRESTCatalogHandler::authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const
{
    /// Only the URI is parsed here, so the body stays available for the route handlers.
    HTMLForm params(server.context()->getSettingsRef(), request);
    /// No fixed user: every request must carry its own credentials.
    const HTTPHandlerConnectionConfig connection_config;
    /// Each request gets a fresh handler, so partial multi-step credentials do not survive a 401 anyway.
    std::unique_ptr<Credentials> request_credentials;
    if (!authenticateUserByHTTP(request, params, response, session, request_credentials, connection_config, server.context(), log))
        return nullptr;

    auto context = session.makeQueryContext();
    context->setCurrentQueryId("");
    return context;
}

std::optional<String> IcebergRESTCatalogHandler::readRequestBody(HTTPServerRequest & request, HTTPServerResponse & response, size_t max_size)
{
    String body;
    /// Read one byte past the limit, otherwise an oversized body is silently truncated.
    LimitReadBuffer limited_stream(*request.getStream(), LimitReadBuffer::Settings{.read_no_more = max_size + 1});
    readStringUntilEOF(body, limited_stream);

    if (body.size() > max_size)
    {
        /// The rest of the body stays unread, so the connection cannot be reused for the next request.
        response.setKeepAlive(false);
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_REQUEST_ENTITY_TOO_LARGE,
            "RequestEntityTooLargeException",
            fmt::format("Request body must not exceed {} bytes", max_size));
        return {};
    }

    return body;
}

void IcebergRESTCatalogHandler::sendJSON(
    HTTPServerResponse & response, const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);

    setResponseDefaultHeaders(response);
    response.setStatus(status);
    response.setContentType("application/json");
    *response.send() << oss.str();
}

void IcebergRESTCatalogHandler::sendError(
    HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message)
{
    /// ErrorModel from the Iceberg REST specification.
    Poco::JSON::Object error;
    error.set("message", message);
    error.set("type", type);
    error.set("code", static_cast<int>(status));

    Poco::JSON::Object result;
    result.set("error", error);
    sendJSON(response, result, status);
}

void IcebergRESTCatalogHandler::sendNoSuchNamespace(HTTPServerResponse & response, const IcebergNamespaceName & name)
{
    sendError(
        response,
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchNamespaceException",
        fmt::format("Namespace does not exist: {}", joinNamespace(name)));
}

void IcebergRESTCatalogHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    try
    {
        Session session(server.context(), ClientInfo::Interface::ICEBERG_REST_CATALOG, request.isSecure());
        auto context = authenticateUser(request, response, session);
        if (!context)
            return; /// 401 with `WWW-Authenticate` is already sent.

        Poco::URI uri;
        std::vector<std::string> segments;
        try
        {
            uri = Poco::URI(request.getURI());
            uri.getPathSegments(segments);
        }
        catch (const Poco::Exception &)
        {
            sendError(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "Malformed request URI");
            return;
        }

        auto match = matchIcebergRESTRoute(request.getMethod(), segments);
        if (!match)
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("No route for {} {}", request.getMethod(), uri.getPath()));
            return;
        }

        /// The spec reserves 406 UnsupportedOperationResponse for operations the server does not support.
        if (!match->route->implemented)
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                "UnsupportedOperationException",
                fmt::format("Not implemented: {}", toString(match->route->operation)));
            return;
        }

        /// `GET /v1/config` has no prefix. It gets the warehouse from the query string instead.
        if (match->route->operation == IcebergRESTOperation::GetConfig)
        {
            handleGetConfig(uri, response);
            return;
        }

        const auto & prefix = match->path_params.at("prefix");
        const auto warehouse = warehouses->tryGet(prefix);
        if (!warehouse)
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("Unknown prefix: {}", prefix));
            return;
        }

        switch (match->route->operation)
        {
            case IcebergRESTOperation::ListNamespaces:
                handleListNamespaces(*warehouse, uri, response);
                return;
            case IcebergRESTOperation::CreateNamespace:
                handleCreateNamespace(*warehouse, request, response, *context);
                return;
            case IcebergRESTOperation::NamespaceExists:
                handleNamespaceExists(*warehouse, *match, response);
                return;
            case IcebergRESTOperation::UpdateNamespaceProperties:
                handleUpdateNamespaceProperties(*warehouse, *match, request, response, *context);
                return;
            default:
                sendError(
                    response,
                    Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                    "UnsupportedOperationException",
                    fmt::format("Not implemented: {}", toString(match->route->operation)));
                return;
        }
    }
    catch (...)
    {
        auto status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
        String type = "InternalServerError";
        String message = "Internal server error";

        const int code = getCurrentExceptionCode();
        if (code == ErrorCodes::BAD_ARGUMENTS)
        {
            status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
            type = "BadRequestException";
            message = getCurrentExceptionMessage(false);
        }
        /// `AccessControl` reports a wrong password for the `default` user as `REQUIRED_PASSWORD`.
        else if (code == ErrorCodes::AUTHENTICATION_FAILED || code == ErrorCodes::REQUIRED_PASSWORD)
        {
            status = Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED;
            type = "NotAuthorizedException";
            message = getCurrentExceptionMessage(false);
        }
        else if (code == ErrorCodes::ACCESS_DENIED || code == ErrorCodes::READONLY || code == ErrorCodes::QUERY_IS_PROHIBITED)
        {
            status = Poco::Net::HTTPResponse::HTTP_FORBIDDEN;
            type = "ForbiddenException";
            message = getCurrentExceptionMessage(false);
        }
        /// The store handles expected Keeper errors. Anything else means the store is unavailable.
        else if (code == ErrorCodes::KEEPER_EXCEPTION || code == ErrorCodes::NO_ZOOKEEPER)
        {
            status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            type = "ServiceUnavailableException";
            message = "Catalog storage is unavailable";
        }

        tryLogCurrentException(log, "Failed to process Iceberg REST catalog request");
        try
        {
            if (!response.sent())
            {
                /// Consume an unread POST body, so the connection can be reused for the next request.
                drainRequestIfNeeded(request, response);
                sendError(response, status, type, message);
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot send exception to client");
        }
    }
}

void IcebergRESTCatalogHandler::handleGetConfig(const Poco::URI & uri, HTTPServerResponse & response) const
{
    auto requested_warehouse = getQueryParameter(uri, "warehouse");
    if (!requested_warehouse)
    {
        sendError(
            response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "This server requires the warehouse query parameter");
        return;
    }

    const auto warehouse = warehouses->tryGet(*requested_warehouse);
    if (!warehouse)
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
            "NoSuchWarehouseException",
            fmt::format("Unknown warehouse: {}", *requested_warehouse));
        return;
    }

    Poco::JSON::Object defaults;
    Poco::JSON::Object overrides;
    Poco::JSON::Array endpoints;


    overrides.set("prefix", warehouse->name);
    for (const auto & route : getIcebergRESTRoutes())
    {
        if (!route.implemented)
            continue;
        endpoints.add(fmt::format("{} /{}", route.method, fmt::join(route.pattern, "/")));
    }

    Poco::JSON::Object result;
    result.set("defaults", defaults);
    result.set("overrides", overrides);
    result.set("endpoints", endpoints);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleListNamespaces(const IcebergRESTCatalogWarehouse & warehouse, const Poco::URI & uri, HTTPServerResponse & response) const
{
    IcebergNamespaceName parent;
    if (auto parent_param = getQueryParameter(uri, "parent"); parent_param && !parent_param->empty())
    {
        parent = splitNamespace(*parent_param);
        if (!warehouse.store->namespaceExists(parent))
        {
            sendNoSuchNamespace(response, parent);
            return;
        }
    }

    Poco::JSON::Array namespaces;
    for (const auto & name : warehouse.store->listNamespaces(parent))
        namespaces.add(stringsToJSONArray(name));

    Poco::JSON::Object result;
    result.set("namespaces", namespaces);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto name = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.store->namespaceExists(name))
    {
        sendNoSuchNamespace(response, name);
        return;
    }

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
    response.send();
}

void IcebergRESTCatalogHandler::checkDDLAllowed(const Context & context, const String & action)
{
    const auto & settings = context.getSettingsRef();
    if (settings[Setting::readonly])
        throw Exception(ErrorCodes::READONLY, "Cannot {} in readonly mode", action);
    if (!settings[Setting::allow_ddl])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Cannot {}. DDL queries are prohibited for the user", action);
}

void IcebergRESTCatalogHandler::handleCreateNamespace(const IcebergRESTCatalogWarehouse & warehouse, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const
{
    checkDDLAllowed(context, "create namespace");

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    IcebergNamespaceName name;
    std::map<String, String> properties;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(*body).extract<Poco::JSON::Object::Ptr>();

        const auto namespace_array = json->getArray("namespace");
        if (!namespace_array)
            throw Poco::Exception("'namespace' must be an array");

        for (const auto & level : *namespace_array)
            name.push_back(level.extract<String>());

        if (json->has("properties"))
        {
            const auto properties_object = json->getObject("properties");
            if (!properties_object)
                throw Poco::Exception("'properties' must be an object");
            for (const auto & [key, value] : *properties_object)
                properties[key] = value.extract<String>();
        }
    }
    catch (const Poco::Exception & e)
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create namespace request: {}", e.displayText()));
        return;
    }
    validateNamespace(name);

    if (!warehouse.store->createNamespace(name, properties))
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_CONFLICT,
            "AlreadyExistsException",
            fmt::format("Namespace already exists: {}", joinNamespace(name)));
        return;
    }

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(name));
    result.set("properties", properties_json);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleUpdateNamespaceProperties(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const
{
    checkDDLAllowed(context, "update namespace properties");

    const auto name = splitNamespace(match.path_params.at("namespace"));

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    std::map<String, String> updates;
    std::set<String> removals;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(*body).extract<Poco::JSON::Object::Ptr>();

        if (json->has("updates"))
        {
            const auto updates_object = json->getObject("updates");
            if (!updates_object)
                throw Poco::Exception("'updates' must be an object");
            for (const auto & [key, value] : *updates_object)
                updates[key] = value.extract<String>();
        }

        if (json->has("removals"))
        {
            const auto removals_array = json->getArray("removals");
            if (!removals_array)
                throw Poco::Exception("'removals' must be an array");
            for (const auto & key : *removals_array)
                removals.insert(key.extract<String>());
        }
    }
    catch (const Poco::Exception & e)
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed update namespace properties request: {}", e.displayText()));
        return;
    }

    const auto conflict = std::ranges::find_if(removals, [&](const auto & key) { return updates.contains(key); });
    if (conflict != removals.end())
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_UNPROCESSABLE_ENTITY,
            "UnprocessableEntityException",
            fmt::format("Property key '{}' is in both 'updates' and 'removals'", *conflict));
        return;
    }

    const auto result = warehouse.store->updateNamespaceProperties(name, updates, removals);
    if (!result)
    {
        sendNoSuchNamespace(response, name);
        return;
    }

    Poco::JSON::Object result_json;
    result_json.set("updated", stringsToJSONArray(result->updated));
    result_json.set("removed", stringsToJSONArray(result->removed));
    result_json.set("missing", stringsToJSONArray(result->missing));
    sendJSON(response, result_json, Poco::Net::HTTPResponse::HTTP_OK);
}

}
