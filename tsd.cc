/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <thread>
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
using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
using csce662::ServerInfo;
using csce662::SNSService;
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

struct Client
{
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client *> client_followers;
  std::vector<Client *> client_following;
  ServerReaderWriter<Message, Message> *stream = 0;
  pthread_t handle = -1;
  bool operator==(const Client &c1) const
  {
    return (username == c1.username);
  }
};

// Vector that stores every client that has been created
std::vector<Client *> client_db;

// Helper function used to find a Client object given its username
int find_user(std::string username)
{
  int index = 0;
  for (Client *c : client_db)
  {
    if (c->username == username)
      return index;
    index++;
  }
  return -1;
}

void keep_alive(int cluster_id, int server_id, std::string coord_ip, std::string coord_port, std::string port_no)
{
  /*
  send keep-alive messages to the coordinator
  */
  // create coordinator stub
  log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id) + "]" + "connecting to coordinator...");
  std::string login_info = coord_ip + ":" + coord_port;
  // create coordinator stub
  auto coord_stub = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(
      grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

  while (true)
  {
    // set parameters
    ClientContext context;
    ServerInfo info;
    Confirmation conf;

    info.set_clusterid(cluster_id);
    info.set_serverid(server_id);
    info.set_hostname("localhost");
    info.set_port(port_no);
    info.set_type("server");

    // send keep alive messages every three seconds
    log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id) + "]" + "sending heartbeat...");
    // send heartbeat to the coordinator
    Status status = coord_stub->Heartbeat(&context, info, &conf);
    // wait for 10 seconds before sending the next keep-alive message
    sleep(10);
  }
  return;
}

bool register_server_with_coordinator(int cluster_id, int server_id, std::string coord_ip,
                                      std::string coord_port, std::string port_no)
{
  // create coordinator stub
  log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id) + "]" + "connecting to coordinator...");
  std::string login_info = coord_ip + ":" + coord_port;
  auto coord_stub = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(
      grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

  // set parameters
  ClientContext context;
  ServerInfo info;
  Confirmation conf;

  info.set_clusterid(cluster_id);
  info.set_serverid(server_id);
  info.set_hostname("localhost");
  info.set_port(port_no);
  info.set_type("server");

  // invoke coordinator's create api to register server
  Status status = coord_stub->Create(&context, info, &conf);

  if (status.ok())
  {
    // if confirmation is true, server registered.
    if (conf.status())
    {
      log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id) + "]" + "Registered server: localhost:" + port_no);
      std::thread ka(keep_alive, cluster_id, server_id, coord_ip, coord_port, port_no);
      ka.detach();
      return true;
    }
    else
    {
      // if confirmation is false, the server is already registered and active.
      log(ERROR, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id) + "]" + "Server is already registered and active...");
      return false;
    }
  }
  return false;
}

// read timeline from file and return as set
std::set<std::string> read_timeline_from_file(std::string filename)
{
  std::set<std::string> timeline;
  std::ifstream tl(filename);
  std::string post;

  while (getline(tl, post))
  {
    timeline.insert(post);
  }

  return timeline;
}

// convert timeline string to Message format
Message create_msg(std::string post)
{
  Message msg;
  int first = post.find('|');
  int second = post.find('@');
  msg.set_username(post.substr(first + 1, second - first - 1));
  msg.set_msg(post.substr(second + 1, std::string::npos));

  // Convert time from string format to google.protobuf.Timestamp
  // and set the timestamp
  struct std::tm tm;
  memset(&tm, 0, sizeof(std::tm));
  strptime(post.substr(0, first).c_str(), "%F %T", &tm);
  google::protobuf::Timestamp *msg_time =
      new google::protobuf::Timestamp();
  msg_time->set_seconds(mktime(&tm));
  msg_time->set_nanos(0);

  msg.set_allocated_timestamp(msg_time);
  return msg;
}

// comparator to sort timelines based on time
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

// sort timelines based on time
std::set<std::string> sort_posts(std::set<std::string> posts)
{
  std::vector<std::string> sorted_posts(posts.begin(), posts.end());
  sort(sorted_posts.begin(), sorted_posts.end(), comparator);
  return std::set<std::string>(sorted_posts.begin(), sorted_posts.end());
}

// monitor timeline files and update user with latest timelines
void monitor_timeline_file(std::string filename, Client *user)
{

  std::set<std::string> old_timeline = read_timeline_from_file(filename);
  while (1)
  {
    sleep(5);
    std::set<std::string> cur_timeline = read_timeline_from_file(filename);
    std::set<std::string> posts;
    std::set_difference(cur_timeline.begin(), cur_timeline.end(),
                        old_timeline.begin(), old_timeline.end(),
                        std::inserter(posts, posts.end()));

    if (!user->stream)
      return;
    if (!posts.empty())
    {
      log(INFO, "Timeline updated. Updating user's stream")
          std::set<std::string>
              sorted_posts = sort_posts(posts);
      for (std::string post : sorted_posts)
        user->stream->Write(create_msg(post));
      old_timeline = cur_timeline;
    }
  }
}

class SNSServiceImpl final : public SNSService::Service
{

public:
  std::string cluster_id;
  std::string server_id;
  std::string coord_ip;
  std::string coord_port;

  // helper function to get slave server's stub
  std::shared_ptr<SNSService::Stub> get_slave_stub()
  {
    std::shared_ptr<CoordService::Stub> stub_ = std::shared_ptr<CoordService::Stub>(
        CoordService::NewStub(grpc::CreateChannel(
            coord_ip + ":" + coord_port, grpc::InsecureChannelCredentials())));

    ClientContext context;
    ID id;
    id.set_id(stoi(cluster_id));
    ServerInfo slave;
    Status status = stub_->GetSlaveServer(&context, id, &slave);

    if (slave.serverid() == -1 || slave.serverid() == stoi(server_id))
    {
      log(INFO, "This is slave server.") return nullptr;
    }

    log(INFO, "Slave server: " + slave.hostname() + ":" + slave.port());
    auto slave_stub_ = std::shared_ptr<SNSService::Stub>(SNSService::NewStub(
        grpc::CreateChannel(
            slave.hostname() + ":" + slave.port(), grpc::InsecureChannelCredentials())));
    return slave_stub_;
  }

  // helper function to get users from users.txt
  void get_all_users()
  {
    std::string filename = "C" + cluster_id + "_S" + server_id + "_users.txt";
    std::ifstream all_users(filename);

    std::string username;
    while (getline(all_users, username))
    {
      if (find_user(username) == -1)
      {
        Client *user = new Client();
        user->username = username;
        client_db.push_back(user);
      }
    }

    all_users.close();
  }

  // helper function to update users.txt
  void set_all_users()
  {
    std::string input("");
    for (Client *user : client_db)
      input += user->username + "\n";

    std::ofstream all_users("C" + cluster_id + "_S" + server_id + "_users.txt");
    all_users << input;
    all_users.close();
  }

  // helper function to get follower/following relation from follow.txt
  void get_follower_following_relation()
  {
    std::ifstream follow_file("C" + cluster_id + "_S" + server_id + "_follow.txt");

    for (Client *user : client_db)
    {
      user->client_followers.clear();
      user->client_following.clear();
    }

    std::string user_name, follower_name;

    while (follow_file >> user_name >> follower_name)
    {
      Client *user = client_db[find_user(user_name)];
      Client *follower = client_db[find_user(follower_name)];
      user->client_followers.push_back(follower);
      follower->client_following.push_back(user);
    }

    follow_file.close();
  }

  // helper function to update follow.txt with latest follower/following relation
  void set_follower_following_relation()
  {
    std::string filename = "C" + cluster_id + "_S" + server_id + "_follow.txt";

    std::string input("");
    for (Client *user : client_db)
    {
      for (Client *follower : user->client_followers)
      {
        input += user->username + " " + follower->username + "\n";
      }
    }

    std::ofstream follow_file(filename);
    follow_file << input;
    follow_file.close();
  }

  Status List(ServerContext *context, const Request *request, ListReply *list_reply) override
  {
    log(INFO, "Serving List Request from: " + request->username() + "\n");

    // get latest users and follower/following relation
    get_all_users();
    get_follower_following_relation();

    Client *user = client_db[find_user(request->username())];

    int index = 0;
    for (Client *user : client_db)
    {
      list_reply->add_all_users(user->username);
    }
    std::vector<Client *>::const_iterator it;
    for (it = user->client_followers.begin(); it != user->client_followers.end(); it++)
    {
      list_reply->add_followers((*it)->username);
    }
    return Status::OK;
  }

  Status Follow(ServerContext *context, const Request *request, Reply *reply) override
  {

    // get latest users and follower/following relation
    get_all_users();
    get_follower_following_relation();

    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    log(INFO, "Serving Follow Request from: " + username1 + " for: " + username2 + "\n");

    int join_index = find_user(username2);
    if (join_index < 0 || username1 == username2)
      reply->set_msg("Join Failed -- Invalid Username");
    else
    {
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[join_index];
      if (std::find(user1->client_following.begin(), user1->client_following.end(),
                    user2) != user1->client_following.end())
      {

        reply->set_msg("Join Failed -- Already Following User");
        return Status::OK;
      }
      user1->client_following.push_back(user2);
      user2->client_followers.push_back(user1);
      reply->set_msg("Follow Successful");
    }

    set_follower_following_relation();
    // call slave's Follow()
    auto slave_stub_ = get_slave_stub();
    if (slave_stub_)
    {
      log(INFO, "Call slave's Follow()");
      ClientContext context;
      Reply reply;
      slave_stub_->Follow(&context, *request, &reply);
    }

    return Status::OK;
  }

  Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override
  {
    //   std::string username1 = request->username();
    //   std::string username2 = request->arguments(0);
    //   log(INFO,"Serving Unfollow Request from: " + username1 + " for: " + username2);

    //   int leave_index = find_user(username2);
    //   if(leave_index < 0 || username1 == username2) {
    //     reply->set_msg("Unknown follower");
    //   } else{
    //     Client *user1 = client_db[find_user(username1)];
    //     Client *user2 = client_db[leave_index];
    //     if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) == user1->client_following.end()){
    // reply->set_msg("You are not a follower");
    //       return Status::OK;
    //     }

    //     user1->client_following.erase(find(user1->client_following.begin(), user1->client_following.end(), user2));
    //     user2->client_followers.erase(find(user2->client_followers.begin(), user2->client_followers.end(), user1));
    //     reply->set_msg("UnFollow Successful");
    //   }

    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext *context, const Request *request, Reply *reply) override
  {

    get_all_users();

    std::string username = request->username();
    log(INFO, "Serving Login Request: " + username + "\n");

    int user_index = find_user(username);

    if (user_index < 0)
    {
      Client *c = new Client();
      c->username = username;
      client_db.push_back(c);
      reply->set_msg("Login Successful!");
    }
    else
    {
      Client *user = client_db[user_index];

      user->stream = 0;
      std::string msg = "Welcome Back " + user->username;
      reply->set_msg(msg);
      user->connected = true;
      if (user->handle != -1)
      {
        pthread_cancel(user->handle);
        user->handle = -1;
      }
    }

    set_all_users();
    // call slave's Login()
    auto slave_stub_ = get_slave_stub();
    if (slave_stub_)
    {
      log(INFO, "Call slave's Login()");
      ClientContext context;
      Reply reply;
      slave_stub_->Login(&context, *request, &reply);
    }

    return Status::OK;
  }

  Status Timeline(ServerContext *context,
                  ServerReaderWriter<Message, Message> *stream) override
  {
    log(INFO, "Serving Timeline Request");
    Message message;
    Client *c;
    while (stream->Read(&message))
    {
      get_all_users();
      get_follower_following_relation();

      std::string username = message.username();
      int user_index = find_user(username);
      c = client_db[user_index];

      // Write the current message to "username.txt"
      std::string filename = "C" + cluster_id + "_S" + server_id + "_" + username + ".txt";

      // Convert timestamp in the format <yyyy-mm-dd hh:mm:ss>
      google::protobuf::Timestamp timestamp = message.timestamp();
      std::time_t time = google::protobuf::util::TimeUtil::TimestampToTimeT(
          timestamp);
      char time_str[20];
      strftime(time_str, 20, "%F %T", localtime(&time));
      std::string fileinput = std::string(time_str) + "|" + message.username() + "@" + message.msg();

      //"Set Stream" is the default message from the client to initialize the stream
      if (message.msg() != "Set Stream")
      {
        std::ifstream in(filename);
        std::stringstream buffer;
        buffer << in.rdbuf();
        in.close();

        std::ofstream user_file(filename);
        user_file << buffer.str() + fileinput;
        user_file.close();
      }
      // If message = "Set Stream", print the first 20 chats from the people you follow
      else
      {
        if (c->stream == 0)
          c->stream = stream;

        std::string line;
        std::vector<std::string> newest_20;
        std::string file = "C" + cluster_id + "_S" + server_id + "_" + username + "_following.txt";
        std::ifstream in(file);
        int count = 0;

        // thread to monitor timeline updates
        std::thread update([file, c]
                           { monitor_timeline_file(file, c); });
        c->handle = update.native_handle();
        update.detach();

        // Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
        while (getline(in, line))
        {
          newest_20.push_back(line);
        }
        // Send the newest messages to the client to be displayed
        if (newest_20.size() >= 20)
        {
          for (int i = newest_20.size() - 20; i < newest_20.size(); ++i)
          {
            stream->Write(create_msg(newest_20[i]));
          }
        }
        else
        {
          for (int i = 0; i < newest_20.size(); ++i)
          {
            stream->Write(create_msg(newest_20[i]));
          }
        }
        continue;
      }
      // Send the message to each follower's stream
      std::vector<Client *>::const_iterator it;
      for (it = c->client_followers.begin(); it != c->client_followers.end(); it++)
      {
        Client *temp_client = *it;

        // For each of the current user's followers, put the message in their following.txt file
        std::string temp_username = temp_client->username;
        std::string temp_file = "C" + cluster_id + "_S" + server_id + "_" + temp_username + "_following.txt";

        std::ifstream in(temp_file);
        std::stringstream buffer;
        buffer << in.rdbuf();
        in.close();

        std::ofstream following_file(temp_file);
        following_file << buffer.str() + fileinput;
        following_file.close();
      }

      // call slave's Timeline()
      auto slave_stub_ = get_slave_stub();
      if (slave_stub_)
      {
        log(INFO, "Call slave's timeline");
        ClientContext ctx;
        std::shared_ptr<grpc::ClientReaderWriter<Message, Message>> w(slave_stub_->Timeline(&ctx));
        w->Write(message);
      }
    }

    // If the client disconnected from Chat Mode, set connected to false
    //  c->connected = false;
    return Status::OK;
  }
};

void RunServer(int cluster_id,
               int server_id,
               std::string coord_ip,
               std::string coord_port,
               std::string port_no)
{

  std::string server_address = "0.0.0.0:" + port_no;
  SNSServiceImpl service;
  service.cluster_id = std::to_string(cluster_id);
  service.server_id = std::to_string(server_id);
  service.coord_ip = coord_ip;
  service.coord_port = coord_port;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::shared_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  // register server
  bool status = register_server_with_coordinator(cluster_id, server_id, coord_ip, coord_port, port_no);
  // if server is already registered and alive, exit...
  if (!status)
  {
    std::cout << "Exiting...\n";
    return;
  }

  server->Wait();
}

int main(int argc, char **argv)
{

  int cluster_id;
  int server_id;
  std::string coord_ip;
  std::string coord_port;
  std::string port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1)
  {
    switch (opt)
    {
    case 'c':
      cluster_id = atoi(optarg);
      break;
    case 's':
      server_id = atoi(optarg);
      break;
    case 'h':
      coord_ip = optarg;
      break;
    case 'k':
      coord_port = optarg;
      break;
    case 'p':
      port = optarg;
      break;
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(cluster_id, server_id, coord_ip, coord_port, port);

  return 0;
}
