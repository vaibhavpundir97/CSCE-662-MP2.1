#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"

using csce662::Confirmation;
using csce662::CoordService;
using csce662::ID;
using csce662::ServerInfo;
using csce662::ServerList;
using csce662::SNSService;
using csce662::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

struct zNode
{
  int serverID;
  std::string hostname;
  std::string port;
  std::string type;
  std::time_t last_heartbeat;
  bool missed_heartbeat;
  bool isActive();
};

// potentially thread safe
std::mutex v_mutex;
std::vector<zNode *> cluster1;
std::vector<zNode *> cluster2;
std::vector<zNode *> cluster3;

// func declarations
zNode *findServer(int cluster_id, int server_id, std::string type);
zNode *getMaster(int cluster_id);
zNode *getSlave(int cluster_id);
std::vector<zNode *> get_cluster(int cluster_id);
std::time_t getTimeNow();
void checkHeartbeat();

bool zNode::isActive()
{
  bool status = false;
  if (!missed_heartbeat)
  {
    status = true;
  }
  else if (difftime(getTimeNow(), last_heartbeat) < 20)
  {
    status = true;
  }
  return status;
}

class CoordServiceImpl final : public CoordService::Service
{

  Status Heartbeat(ServerContext *context, const ServerInfo *serverinfo, Confirmation *confirmation) override
  {
    log(INFO, "[c_id:" + std::to_string(serverinfo->clusterid()) + "::" +
                  serverinfo->type() + ":s_id:" + std::to_string(serverinfo->serverid()) + "] Got Heartbeat!");

    // Your code here
    // get server object from the cluster
    zNode *server = findServer(serverinfo->clusterid(), serverinfo->serverid(), serverinfo->type());
    // update last_heartbeat and set missed_heartbeat as false
    // locking since checkHeartbeat() function can modify the same parallelly
    v_mutex.lock();
    server->last_heartbeat = getTimeNow();
    server->missed_heartbeat = false;
    v_mutex.unlock();
    return Status::OK;
  }

  // function returns the server information for requested client id
  // this function assumes there are always 3 clusters and has math
  // hardcoded to represent this.
  Status GetServer(ServerContext *context, const ID *id, ServerInfo *serverinfo) override
  {
    log(INFO, "Got GetServer for clientID: " + std::to_string(id->id()));
    // get clusterID based on cliendID
    int clusterID = ((id->id() - 1) % 3) + 1;
    // since each cluster has only one server, serverID will be 1
    // int serverID = 1;

    // Your code here
    // get server object from the cluster
    zNode *s1 = getMaster(clusterID);
    zNode *s2 = getSlave(clusterID);
    zNode *s = s1;

    if (s1)
    {
      // if master is not active,
      // make slave master and return serverinfo
      if (!s1->isActive())
      {
        s1->type = "slave";
        s2->type = "master";
        s = s2;
      }
      serverinfo->set_clusterid(clusterID);
      serverinfo->set_serverid(s->serverID);
      serverinfo->set_hostname(s->hostname);
      serverinfo->set_port(s->port);
      serverinfo->set_type(s->type);
    }
    else
    {
      // server not found
      log(INFO, "Couldn't find server for clientID: " + std::to_string(id->id()));
      serverinfo->set_clusterid(-1);
    }
    return Status::OK;
  }

  Status Create(ServerContext *context, const ServerInfo *serverinfo, Confirmation *confirmation) override
  {
    // check if server already exists
    zNode *s = findServer(serverinfo->clusterid(), serverinfo->serverid(), serverinfo->type());
    if (s)
    {
      // if inactive, the server is restarted after crash
      log(INFO, "[c_id:" + std::to_string(serverinfo->clusterid()) + "::s_id:" +
                    std::to_string(serverinfo->serverid()) + "] Server was inactive!");

      zNode *synch = findServer(serverinfo->clusterid(), serverinfo->clusterid(), "synchronizer");
      std::string login_info = synch->hostname + ":" + synch->port;
      std::shared_ptr<SynchService::Stub> stub_;
      stub_ = std::shared_ptr<SynchService::Stub>(SynchService::NewStub(
          grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

      ClientContext ctx;
      ServerInfo info;
      info.set_clusterid(serverinfo->clusterid());
      info.set_serverid(serverinfo->serverid());
      info.set_hostname(serverinfo->hostname());
      info.set_port(serverinfo->port());
      Confirmation conf;
      log(INFO, "Synchronizing server: " + std::to_string(serverinfo->serverid()) + "-" + serverinfo->port());
      Status status = stub_->ResynchServer(&ctx, info, &conf);

      if (status.ok())
        confirmation->set_status(true);
      return Status::OK;
    }

    // if server is not present
    log(INFO, "[c_id:" + std::to_string(serverinfo->clusterid()) + "::s_id:" +
                  std::to_string(serverinfo->serverid()) + "] Registering server!");
    // create server object and add to the cluster
    s = new zNode();
    s->serverID = serverinfo->serverid();
    s->hostname = serverinfo->hostname();
    s->port = serverinfo->port();
    s->last_heartbeat = getTimeNow();
    s->missed_heartbeat = false;

    if (serverinfo->type() == "server")
    {
      zNode *master = getMaster(serverinfo->clusterid());
      if (master && master->isActive())
        s->type = "slave";
      else
        s->type = "master";
    }
    else
      s->type = "synchronizer";

    switch (serverinfo->clusterid())
    {
    case 1:
      cluster1.push_back(s);
      break;
    case 2:
      cluster2.push_back(s);
      break;
    case 3:
      cluster3.push_back(s);
      break;
    }

    confirmation->set_status(true);
    return Status::OK;
  }

  // if master is down, exchange roles with slave
  Status Exists(ServerContext *context, const ID *id, Confirmation *confirmation) override
  {
    int clusterID = ((id->id() - 1) % 3) + 1;
    zNode *master = getMaster(clusterID);
    if (master && master->isActive())
      confirmation->set_status(true);
    else
    {
      confirmation->set_status(false);
      zNode *slave = getSlave(clusterID);
      slave->type = "master";
      master->type = "slave";
    }
    return Status::OK;
  }

  Status GetSlaveServer(ServerContext *context, const ID *id, ServerInfo *serverinfo) override
  {
    log(INFO, "Fetch slave from cluster: " + id->id());
    zNode *slave = getSlave(id->id());
    if (slave && slave->isActive())
    {
      serverinfo->set_serverid(slave->serverID);
      serverinfo->set_hostname(slave->hostname);
      serverinfo->set_port(slave->port);
    }
    else
    {
      serverinfo->set_clusterid(-1);
    }

    return Status::OK;
  }

  Status GetAllFollowerServers(ServerContext *context, const ID *id, ServerList *list) override
  {
    log(INFO, "Get synchronizers");
    int synchID;
    int list_size = 0;
    for (int clusterID = 1; clusterID <= 3; ++clusterID)
    {
      if (id->id() == clusterID)
        continue;
      synchID = clusterID;
      zNode *sync = findServer(clusterID, synchID, "synchronizer");
      if (!sync)
        continue;
      list->add_serverid(synchID);
      list->add_hostname(sync->hostname);
      list->add_port(sync->port);
      list->add_type("synchronizer");
      ++list_size;
    }

    list->set_size(list_size);
    return Status::OK;
  }
};

void RunServer(std::string port_no)
{
  // start thread to check heartbeats
  std::thread hb(checkHeartbeat);
  // localhost = 127.0.0.1
  std::string server_address("127.0.0.1:" + port_no);
  CoordServiceImpl service;
  // grpc::EnableDefaultHealthCheckService(true);
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::shared_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char **argv)
{

  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1)
  {
    switch (opt)
    {
    case 'p':
      port = optarg;
      break;
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("coordinator-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);
  return 0;
}

void checkHeartbeat()
{
  while (true)
  {
    // check servers for heartbeat > 10
    // if true turn missed heartbeat = true
    //  Your code below
    // std::cout << "Check server heartbeats...\n";
    //  iterate over all the clusters
    for (auto &cluster : {&cluster1, &cluster2, &cluster3})
    {
      // iterate over all the servers in cluster
      for (auto &s : *cluster)
      {
        if (difftime(getTimeNow(), s->last_heartbeat) > 20)
        {
          if (!s->missed_heartbeat)
          {
            v_mutex.lock();
            s->missed_heartbeat = true;
            s->last_heartbeat = getTimeNow();
            v_mutex.unlock();
          }
          else
          {
          }
        }
      }
    }
    sleep(3);
  }
}

std::time_t getTimeNow()
{
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

std::vector<zNode *> get_cluster(int cluster_id)
{
  // get cluster based on cluster id
  std::vector<zNode *> cluster;

  switch (cluster_id)
  {
  case 1:
    cluster = cluster1;
    break;
  case 2:
    cluster = cluster2;
    break;
  case 3:
    cluster = cluster3;
    break;
  }

  return cluster;
}

// helper function to get server from cluster based on cluster and server id and type
zNode *findServer(int cluster_id, int server_id, std::string type)
{
  // std::cout << "Finding server object with clusterID: " << std::to_string(cluster_id) <<
  //" and serverID: " << std::to_string(server_id) << std::endl;

  std::vector<zNode *> cluster = get_cluster(cluster_id);

  // find server in cluster
  for (auto &server : cluster)
  {
    if (server->serverID == server_id)
      // std::cout << "Server object found!\n";
      if (type == "server" && (server->type == "master" || server->type == "slave"))
        return server;
      else if (type == server->type)
      {
        return server;
      }
  }
  return nullptr;
}

// get master server from cluster
zNode *getMaster(int cluster_id)
{

  std::vector<zNode *> cluster = get_cluster(cluster_id);

  // find server in cluster
  for (auto &server : cluster)
  {
    if (server->type == "master")
      // std::cout << "Server object found!\n";
      return server;
  }
  return nullptr;
}

// get slave server from cluster
zNode *getSlave(int cluster_id)
{

  std::vector<zNode *> cluster = get_cluster(cluster_id);

  // find server in cluster
  for (auto &server : cluster)
  {
    if (server->type == "slave")
      // std::cout << "Server object found!\n";
      return server;
  }
  return nullptr;
}
