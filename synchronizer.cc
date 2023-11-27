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
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"

namespace fs = std::filesystem;

using csce662::AllUsers;
using csce662::Confirmation;
using csce662::CoordService;
using csce662::FL;
using csce662::ID;
using csce662::ServerInfo;
using csce662::ServerList;
using csce662::SynchService;
using csce662::TL;
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

int synchID = 1;
std::vector<std::string> get_lines_from_file(std::string);
void run_synchronizer(std::string, std::string, std::string, int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl(int, int);
std::vector<std::string> get_fl(int);

class SynchServiceImpl final : public SynchService::Service
{

public:
  std::string coord_ip;
  std::string coord_port;

  Status GetAllUsers(ServerContext *context, const Confirmation *confirmation, AllUsers *allusers) override
  {
    // std::cout<<"Got GetAllUsers"<<std::endl;
    log(INFO, "Got GetAllUsers");
    std::vector<std::string> list = get_all_users_func(synchID);
    // package list
    for (auto s : list)
    {
      allusers->add_users(s);
    }

    // return list
    return Status::OK;
  }

  Status GetTL(ServerContext *context, const ID *id, TL *tl)
  {
    // std::cout<<"Got GetTLFL"<<std::endl;
    log(INFO, "Got GetTL: " + id->id());
    int clientID = id->id();

    std::vector<std::string> synch_tl = get_tl(synchID, clientID);

    // now populate TL tl for return
    for (auto s : synch_tl)
    {
      tl->add_tl(s);
    }
    tl->set_status(true);

    return Status::OK;
  }

  Status GetFL(ServerContext *context, const ID *id, FL *fl)
  {
    // std::cout<<"Got GetTLFL"<<std::endl;
    log(INFO, "Got GetFL");
    int clientID = id->id();

    std::vector<std::string> synch_fl = get_fl(synchID);

    // now populate FL fl for return
    for (auto s : synch_fl)
    {
      fl->add_fl(s);
    }
    fl->set_status(true);

    return Status::OK;
  }

  // helper function to copy files b/w servers of same cluster
  bool _copy(std::string src, std::string dst)
  {
    std::ifstream src_file(src, std::ios::binary);
    if (!src_file.is_open())
    {
      return false;
    }

    std::ofstream dst_file(dst, std::ios::binary | std::ios::trunc);

    dst_file << src_file.rdbuf();

    src_file.close();
    dst_file.close();

    return true;
  }

  Status ResynchServer(ServerContext *context, const ServerInfo *serverinfo, Confirmation *c)
  {
    log(INFO, serverinfo->type() + "(" + std::to_string(serverinfo->serverid()) +
                  ") just restarted and needs to be resynched with counterpart")
        // std::cout << serverinfo->type() << "(" << serverinfo->serverid() <<
        // ") just restarted and needs to be resynched with counterpart" << std::endl;
        std::string backupServerType;

    // YOUR CODE HERE
    std::string target_str = coord_ip + ":" + coord_port;
    std::shared_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

    ClientContext ctx;
    ID id;
    id.set_id(synchID);
    ServerInfo info;
    coord_stub_->GetServer(&ctx, id, &info);
    std::string master_file = "C" + std::to_string(synchID) + "_S" + std::to_string(info.serverid());
    std::string slave_file = "C" + std::to_string(synchID) + "_S" + std::to_string(serverinfo->serverid());

    // copy users & follow files
    _copy(master_file + "_users.txt", slave_file + "_users.txt");
    _copy(master_file + "_follow.txt", slave_file + "_follow.txt");

    // copy timeline files
    std::vector<std::string> users = get_all_users_func(synchID);
    for (std::string u : users)
    {
      _copy(master_file + ".txt", slave_file + ".txt");
      _copy(master_file + "_following.txt", slave_file + "_following.txt");
    }

    return Status::OK;
  }
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
  // localhost = 127.0.0.1
  std::string server_address("127.0.0.1:" + port_no);
  SynchServiceImpl service;
  service.coord_ip = coordIP;
  service.coord_port = coordPort;
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

  std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char **argv)
{

  int opt = 0;
  std::string coordIP;
  std::string coordPort;
  std::string port = "3029";

  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
  {
    switch (opt)
    {
    case 'h':
      coordIP = optarg;
      break;
    case 'k':
      coordPort = optarg;
      break;
    case 'p':
      port = optarg;
      break;
    case 'i':
      synchID = std::stoi(optarg);
      break;
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("synchronizer-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Synchronizer starting...");
  RunServer(coordIP, coordPort, port, synchID);
  return 0;
}

// helper functin to get other synchronizer's stub
std::shared_ptr<SynchService::Stub> get_stub(std::string hostname, std::string port)
{
  std::string login_info = hostname + ":" + port;

  std::shared_ptr<SynchService::Stub> stub_;
  stub_ = std::shared_ptr<SynchService::Stub>(SynchService::NewStub(
      grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

  return stub_;
}

// helper function to write to files
void write_to_file(std::string filepath, std::set<std::string> data)
{

  std::string input("");
  for (auto x : data)
    input += x + "\n";

  std::ofstream file(filepath, std::ios::out | std::ios::trunc);
  file << input;
  file.close();
}

// helper function to fech synchronizer servers
ServerList get_server_list(
    std::shared_ptr<CoordService::Stub> coord_stub_,
    int synchID)
{

  ClientContext ctx;
  ID id;
  id.set_id(synchID);
  ServerList list;
  coord_stub_->GetAllFollowerServers(&ctx, id, &list);

  return list;
}

// sync users across all clusters
void sync_users(std::shared_ptr<CoordService::Stub> coord_stub_, int synchID)
{

  std::set<std::string> user_set;
  ServerList list;

  list = get_server_list(coord_stub_, synchID);

  for (int i = 0; i < list.size(); ++i)
  {
    auto stub = get_stub(list.hostname(i), list.port(i));

    ClientContext context;
    Confirmation conf;
    AllUsers users;
    stub->GetAllUsers(&context, conf, &users);

    for (auto user : users.users())
      user_set.insert(user);
  }

  std::vector<std::string> users = get_all_users_func(synchID);
  for (auto user : users)
    user_set.insert(user);

  std::string s1_users_file = "C" + std::to_string(synchID) + "_S1_users.txt";
  std::string s2_users_file = "C" + std::to_string(synchID) + "_S2_users.txt";
  write_to_file(s1_users_file, user_set);
  write_to_file(s2_users_file, user_set);
}

// sync follower/following relation across cluster
void sync_fl(std::shared_ptr<CoordService::Stub> coord_stub_, int synchID)
{

  std::set<std::string> fl;

  int clientID = -1;

  ServerList list;
  list = get_server_list(coord_stub_, synchID);

  for (int i = 0; i < list.size(); ++i)
  {
    auto stub = get_stub(list.hostname(i), list.port(i));

    ClientContext context;
    ID id;
    id.set_id(clientID);
    FL fl_;
    stub->GetFL(&context, id, &fl_);

    for (auto r : fl_.fl())
      fl.insert(r);
  }

  for (auto r : get_fl(synchID))
    fl.insert(r);

  std::string s1_follow_file = "C" + std::to_string(synchID) + "_S1_follow.txt";
  std::string s2_follow_file = "C" + std::to_string(synchID) + "_S2_follow.txt";
  write_to_file(s1_follow_file, fl);
  write_to_file(s2_follow_file, fl);
}

// helper function to sort posts based on time
bool comparator(std::string p1, std::string p2)
{
  std::string ts1 = p1.substr(0, p1.find('|'));
  std::string ts2 = p2.substr(0, p1.find('|'));

  struct std::tm tm1;
  memset(&tm1, 0, sizeof(std::tm));
  strptime(ts1.c_str(), "%F %T", &tm1);
  std::time_t t1 = mktime(&tm1);

  struct std::tm tm2;
  memset(&tm2, 0, sizeof(std::tm));
  strptime(ts2.c_str(), "%F %T", &tm2);
  std::time_t t2 = mktime(&tm2);

  if (t1 < t2)
    return true;
  return false;
}

// sort timeline posts based on time
std::set<std::string> sort_posts(std::set<std::string> posts)
{
  std::vector<std::string> sorted_posts(posts.begin(), posts.end());
  sort(sorted_posts.begin(), sorted_posts.end(), comparator);
  return std::set<std::string>(sorted_posts.begin(), sorted_posts.end());
}

// sync timeline across all clusters
void sync_tl(std::shared_ptr<CoordService::Stub> coord_stub_, int synchID)
{

  log(INFO, "Synchronizing Timeline");

  std::map<std::string, std::vector<std::string>> rel;
  auto users = get_all_users_func(synchID);
  auto fl = get_fl(synchID);

  std::string user1, user2;
  for (auto user : users)
  {
    if ((((stoi(user) - 1) % 3) + 1) != synchID)
      continue;
    rel.insert({user, std::vector<std::string>()});
    for (auto r : fl)
    {
      std::istringstream iss(r);
      iss >> user1 >> user2;
      if (user == user2)
        rel[user].push_back(user1);
    }
  }

  ServerList list;
  list = get_server_list(coord_stub_, synchID);

  std::map<int, std::shared_ptr<SynchService::Stub>> synch_stubs_;

  for (int i = 0; i < list.size(); ++i)
  {
    auto stub_ = get_stub(list.hostname(i), list.port(i));
    synch_stubs_[list.serverid(i)] = stub_;
  }

  for (auto const &r : rel)
  {
    std::string user = r.first;
    std::vector<std::string> following = r.second;
    log(INFO, "[TL] User: " + user);
    std::set<std::string> tls;
    for (std::string u : following)
    {
      log(INFO, "[TL] Following: " + u);
      int user_synch_id = ((stoi(u) - 1) % 3) + 1;
      if (user_synch_id == synchID)
      {
        for (auto s : get_tl(synchID, stoi(user)))
          tls.insert(s);
        continue;
      }
      ClientContext ctx;
      ID id;
      id.set_id(stoi(user));
      TL tl;
      synch_stubs_[user_synch_id]->GetTL(&ctx, id, &tl);
      for (auto s : tl.tl())
        tls.insert(s);
    }

    std::set<std::string> posts = sort_posts(tls);

    std::string s1_tl_file = "C" + std::to_string(synchID) + "_S1_" + user + "_following.txt";
    std::string s2_tl_file = "C" + std::to_string(synchID) + "_S2_" + user + "_following.txt";
    write_to_file(s1_tl_file, posts);
    write_to_file(s2_tl_file, posts);
  }
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID)
{
  // setup coordinator stub
  // std::cout<<"synchronizer stub"<<std::endl;
  std::string target_str = coordIP + ":" + coordPort;
  std::shared_ptr<CoordService::Stub> coord_stub_;
  coord_stub_ = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
  // std::cout<<"MADE STUB"<<std::endl;

  ServerInfo msg;
  Confirmation c;
  ClientContext context;

  msg.set_clusterid(synchID);
  msg.set_serverid(synchID);
  msg.set_hostname("127.0.0.1");
  msg.set_port(port);
  msg.set_type("synchronizer");
  coord_stub_->Create(&context, msg, &c);

  // send init heartbeat

  // TODO: begin synchronization process
  while (true)
  {
    // change this to 30 eventually
    sleep(20);
    // synch all users file
    // get list of all followers

    // YOUR CODE HERE
    // set up stub
    // send each a GetAllUsers request
    // aggregate users into a list
    // sort list and remove duplicates

    // YOUR CODE HERE

    // for all the found users
    // if user not managed by current synch
    //  ...

    // YOUR CODE HERE

    sync_users(coord_stub_, synchID);
    sync_fl(coord_stub_, synchID);
    sync_tl(coord_stub_, synchID);

    // force update managed users from newly synced users
    // for all users
    //  for(auto i : aggregated_users) {
    // get currently managed users
    // if user IS managed by current synch
    // read their follower lists
    // for followed users that are not managed on cluster
    // read followed users cached timeline
    // check if posts are in the managed tl
    // add post to tl of managed user

    // YOUR CODE HERE
    //     }
    // }
    // }
  }
  return;
}

std::vector<std::string> get_lines_from_file(std::string filename)
{
  std::vector<std::string> users;
  std::string user;
  std::ifstream file;
  file.open(filename);
  if (file.peek() == std::ifstream::traits_type::eof())
  {
    // return empty vector if empty file
    // std::cout<<"returned empty vector bc empty file"<<std::endl;
    file.close();
    return users;
  }
  while (file)
  {
    getline(file, user);

    if (!user.empty())
      users.push_back(user);
  }

  file.close();

  // std::cout<<"File: "<<filename<<" has users:"<<std::endl;
  /*for(int i = 0; i<users.size(); i++){
    std::cout<<users[i]<<std::endl;
  }*/

  return users;
}

bool file_contains_user(std::string filename, std::string user)
{
  std::vector<std::string> users;
  // check username is valid
  users = get_lines_from_file(filename);
  for (int i = 0; i < users.size(); i++)
  {
    // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
    if (user == users[i])
    {
      // std::cout<<"found"<<std::endl;
      return true;
    }
  }
  // std::cout<<"not found"<<std::endl;
  return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
  // read all_users file master and client for correct serverID
  std::string s1_users_file = "C" + std::to_string(synchID) + "_S1_users.txt";
  std::string s2_users_file = "C" + std::to_string(synchID) + "_S2_users.txt";
  // take longest list and package into AllUsers message
  std::vector<std::string> s1_user_list = get_lines_from_file(s1_users_file);
  std::vector<std::string> s2_user_list = get_lines_from_file(s2_users_file);

  if (s1_user_list.size() >= s2_user_list.size())
    return s1_user_list;
  else
    return s2_user_list;
}

std::vector<std::string> get_tl(int synchID, int clientID)
{
  std::string s1_fn = "C" + std::to_string(synchID) + "_S1_" + std::to_string(clientID) + "_following.txt";
  std::string s2_fn = "C" + std::to_string(synchID) + "_S2_" + std::to_string(clientID) + "_following.txt";

  std::vector<std::string> s1 = get_lines_from_file(s1_fn);
  std::vector<std::string> s2 = get_lines_from_file(s2_fn);

  if (s1.size() >= s2.size())
  {
    return s1;
  }
  else
  {
    return s2;
  }
}

std::vector<std::string> get_fl(int synchID)
{
  std::string s1_fn = "C" + std::to_string(synchID) + "_S1_follow.txt";
  std::string s2_fn = "C" + std::to_string(synchID) + "_S2_follow.txt";

  std::vector<std::string> s1 = get_lines_from_file(s1_fn);
  std::vector<std::string> s2 = get_lines_from_file(s2_fn);

  if (s1.size() >= s2.size())
  {
    return s1;
  }
  else
  {
    return s2;
  }
}
