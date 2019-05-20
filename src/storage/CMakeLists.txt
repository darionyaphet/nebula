add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    KeyUtils.cpp
    StorageHttpHandler.cpp
    processors/AddVerticesProcessor.cpp
    processors/AddEdgesProcessor.cpp
    processors/QueryBoundProcessor.cpp
    processors/QueryVertexPropsProcessor.cpp
    processors/QueryEdgePropsProcessor.cpp
    processors/QueryStatsProcessor.cpp
)
add_dependencies(
    storage_service_handler
    base_obj
    storage_thrift_obj
    kvstore_obj
    dataman_obj
    thread_obj
    ws_obj
    stats_obj
)


add_library(
    storage_client OBJECT
    client/StorageClient.cpp
)
add_dependencies(
    storage_client
    base_obj
    storage_thrift_obj
    dataman_obj
)


add_subdirectory(test)
