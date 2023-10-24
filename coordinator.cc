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
//#include<glog/logging.h>
//#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "coordinator.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662c::CoordService;
using csce662c::ServerInfo;
using csce662c::Confirmation;
using csce662c::ID;
//using csce662c::ServerList;
//using csce662c::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;


//func declarations
zNode* findServer(int cluster_id, int server_id);
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive() {
  bool status = false;
  if(!missed_heartbeat) {
    status = true;
  } else if(difftime(getTimeNow(), last_heartbeat) < 10) {
    status = true;
  }
  return status;
}

class CoordServiceImpl final : public CoordService::Service {

  
  Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    std::cout << "Got Heartbeat! clusterid: " << serverinfo->clusterid() << ", " << serverinfo->type() 
    << "(serverid: " << serverinfo->serverid() << ")" << std::endl;

    // Your code here
    zNode* server = findServer(serverinfo->clusterid(), serverinfo->serverid());
    v_mutex.lock();
    server->last_heartbeat = getTimeNow();
    server->missed_heartbeat = false;
    v_mutex.unlock();
    return Status::OK;
  }
  
  //function returns the server information for requested client id
  //this function assumes there are always 3 clusters and has math
  //hardcoded to represent this.
  Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    std::cout<<"Got GetServer for clientID: "<<id->id()<<std::endl;
    int clusterID = ((id->id() - 1) % 3) + 1;
    int serverID = 1;

    // Your code here
    // If server is active, return serverinfo
    zNode* s = findServer(clusterID, serverID);
    if(s->isActive()) {
      serverinfo->set_clusterid(serverID);
      serverinfo->set_serverid(serverID);
      serverinfo->set_hostname(s->hostname);
      serverinfo->set_port(s->port);
      serverinfo->set_type(s->type);
    } else {
      serverinfo->set_clusterid(-1);
    }
    return Status::OK;
  }

  Status Create(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    zNode* s = findServer(serverinfo->clusterid(), serverinfo->clusterid());
    if(s) {
      if(s->isActive()) {
        confirmation->set_status(false);
      } else {
        confirmation->set_status(true);
      }
      return Status::OK;
    }

    s = new zNode();
    s->serverID = serverinfo->serverid();
    s->hostname = serverinfo->hostname();
    s->port = serverinfo->port();
    s->type = serverinfo->type();
    s->last_heartbeat = getTimeNow();
    s->missed_heartbeat = false;

    switch(serverinfo->clusterid()) {
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

  Status Exists(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    // zNode* s = findServer(serverinfo->clusterid(), serverinfo->serverid());
    // if(s && s->isActive())
    return Status::OK;
  }

};

void RunServer(std::string port_no){
  //start thread to check heartbeats
  std::thread hb(checkHeartbeat);
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  CoordServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}



void checkHeartbeat() {
  while(true) {
    //check servers for heartbeat > 10
    //if true turn missed heartbeat = true
    // Your code below
    //std::cout << "Check server heartbeats...\n";
    for(auto& cluster: {&cluster1, &cluster2, &cluster3}) {
      for(auto& s : *cluster) {
        if(difftime(getTimeNow(), s->last_heartbeat) > 10) {
          if(!s->missed_heartbeat) {
            v_mutex.lock();
            s->missed_heartbeat = true;
            s->last_heartbeat = getTimeNow();
            v_mutex.unlock();
          } else {
            
          }
        }
      }
    }
    sleep(3);
  }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}


zNode* findServer(int cluster_id, int server_id) {
  //std::cout << "Finding server object with clusterID: " << std::to_string(cluster_id) <<
  //" and serverID: " << std::to_string(server_id) << std::endl;

  std::vector<zNode*> cluster;

  switch(cluster_id) {
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

  for(auto& server: cluster) {
    if(server->serverID == server_id)
      //std::cout << "Server object found!\n";
      return server;
  }
  return nullptr;
}
