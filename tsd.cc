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
#include <thread>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662c::ServerInfo;
using csce662c::Confirmation;
using csce662c::CoordService;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;

struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

//Helper function used to find a Client object given its username
int find_user(std::string username){
  int index = 0;
  for(Client* c : client_db){
    if(c->username == username)
      return index;
    index++;
  }
  return -1;
}

void keep_alive(int cluster_id, int server_id, std::string coord_ip, std::string coord_port, std::string port_no) {
  /*
  send keep-alive messages to the coordinator
  */
  // create coordinator stub
  log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id)
   + "]" + "connecting to coordinator...");
  std::string login_info = coord_ip + ":" + coord_port;
  // create coordinator stub
  auto coord_stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(
    grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

  while (true) {
    // set parameters
    ClientContext context;
    ServerInfo info;
    Confirmation conf;

    info.set_clusterid(cluster_id);
    info.set_serverid(server_id);
    info.set_hostname("localhost");
    info.set_port(port_no);
    info.set_type("master");

    // send keep alive messages every three seconds
    log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id)
     + "]" + "sending heartbeat...");
    // send heartbeat to the coordinator
    Status status = coord_stub->Heartbeat(&context, info, &conf);
    // wait for 5 seconds before sending the next keep-alive message
    sleep(5);
  }
  return;
}

bool register_server_with_coordinator(int cluster_id, int server_id, std::string coord_ip,
                                      std::string coord_port, std::string port_no) {
  // create coordinator stub
  log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id)
   + "]" + "connecting to coordinator...");
  std::string login_info = coord_ip + ":" + coord_port;
  auto coord_stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(
    grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials())));

  // set parameters
  ClientContext context;
  ServerInfo info;
  Confirmation conf;

  info.set_clusterid(cluster_id);
  info.set_serverid(server_id);
  info.set_hostname("localhost");
  info.set_port(port_no);
  info.set_type("master");

  // invoke coordinator's create api to register server
  Status status = coord_stub->Create(&context, info, &conf);

  if(status.ok()) {
    // if confirmation is true, server registered.
    if(conf.status()) {
      log(INFO, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id)
       + "]" + "Registered server: localhost:" + port_no);
      std::thread ka(keep_alive, cluster_id, server_id, coord_ip, coord_port, port_no);
      ka.detach();
      return true;
    } else {
      // if confirmation is false, the server is already registered and active.
      log(ERROR, "[c_id:" + std::to_string(cluster_id) + "::s_id:" + std::to_string(server_id)
       + "]" + "Server is already registered and active...");
      return false;
    }
  }
  return false;
}

class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    log(INFO,"Serving List Request from: " + request->username()  + "\n");
     
    Client* user = client_db[find_user(request->username())];
 
    int index = 0;
    for(Client* c : client_db){
      list_reply->add_all_users(c->username);
    }
    std::vector<Client*>::const_iterator it;
    for(it = user->client_followers.begin(); it!=user->client_followers.end(); it++){
      list_reply->add_followers((*it)->username);
    }
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    log(INFO,"Serving Follow Request from: " + username1 + " for: " + username2 + "\n");

    int join_index = find_user(username2);
    if(join_index < 0 || username1 == username2)
      reply->set_msg("Join Failed -- Invalid Username");
    else{
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[join_index];      
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) != user1->client_following.end()){
	reply->set_msg("Join Failed -- Already Following User");
        return Status::OK;
      }
      user1->client_following.push_back(user2);
      user2->client_followers.push_back(user1);
      reply->set_msg("Follow Successful");
    }
    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    log(INFO,"Serving Unfollow Request from: " + username1 + " for: " + username2);
 
    int leave_index = find_user(username2);
    if(leave_index < 0 || username1 == username2) {
      reply->set_msg("Unknown follower");
    } else{
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[leave_index];
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) == user1->client_following.end()){
	reply->set_msg("You are not a follower");
        return Status::OK;
      }
      
      user1->client_following.erase(find(user1->client_following.begin(), user1->client_following.end(), user2)); 
      user2->client_followers.erase(find(user2->client_followers.begin(), user2->client_followers.end(), user1));
      reply->set_msg("UnFollow Successful");
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    Client* c = new Client();
    std::string username = request->username();
    log(INFO, "Serving Login Request: " + username + "\n");
    
    int user_index = find_user(username);
    if(user_index < 0){
      c->username = username;
      client_db.push_back(c);
      reply->set_msg("Login Successful!");
    }
    else{
      Client *user = client_db[user_index];
      if(user->connected) {
	log(WARNING, "User already logged on");
        reply->set_msg("you have already joined");
      }
      else{
        std::string msg = "Welcome Back " + user->username;
	reply->set_msg(msg);
        user->connected = true;
      }
    }
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    log(INFO,"Serving Timeline Request");
    Message message;
    Client *c;
    while(stream->Read(&message)) {
      std::string username = message.username();
      int user_index = find_user(username);
      c = client_db[user_index];
 
      //Write the current message to "username.txt"
      std::string filename = username+".txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      google::protobuf::Timestamp temptime = message.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(temptime);
      std::string fileinput = time+" :: "+message.username()+":"+message.msg()+"\n";
      //"Set Stream" is the default message from the client to initialize the stream
      if(message.msg() != "Set Stream")
        user_file << fileinput;
      //If message = "Set Stream", print the first 20 chats from the people you follow
      else {
        if(c->stream==0)
      	  c->stream = stream;
        std::string line;
        std::vector<std::string> newest_twenty;
        std::ifstream in(username+"following.txt");
        // std::ifstream in(username+".txt");
        int count = 0;
        //Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
        while(getline(in, line)){
//          count++;
//          if(c->following_file_size > 20){
//	    if(count < c->following_file_size-20){
//	      continue;
//            }
//          }
          newest_twenty.push_back(line);
        }
        Message new_msg; 
 	      //Send the newest messages to the client to be displayed
        if(newest_twenty.size() >= 40) { 	
          for(int i = newest_twenty.size()-40; i<newest_twenty.size(); i+=2) {
            new_msg.set_msg(newest_twenty[i]);
            stream->Write(new_msg);
          }
        } else {
          for(int i = 0; i<newest_twenty.size(); i+=2) {
            new_msg.set_msg(newest_twenty[i]);
            stream->Write(new_msg);
          }
        }
        //std::cout << "newest_twenty.size() " << newest_twenty.size() << std::endl; 
        continue;
      }
      //Send the message to each follower's stream
      std::vector<Client*>::const_iterator it;
      for(it = c->client_followers.begin(); it!=c->client_followers.end(); it++) {
        Client *temp_client = *it;
      	if(temp_client->stream!=0 && temp_client->connected)
	        temp_client->stream->Write(message);
        //For each of the current user's followers, put the message in their following.txt file
        std::string temp_username = temp_client->username;
        std::string temp_file = temp_username + "following.txt";
        // std::string temp_file = temp_username + ".txt";
	      std::ofstream following_file(temp_file,std::ios::app|std::ios::out|std::ios::in);
	      following_file << fileinput;
        temp_client->following_file_size++;
	      std::ofstream user_file(temp_username + ".txt",std::ios::app|std::ios::out|std::ios::in);
        user_file << fileinput;
      }
    }
    //If the client disconnected from Chat Mode, set connected to false
    c->connected = false;
    return Status::OK;
  }

};

void RunServer(int cluster_id, int server_id, std::string coord_ip, std::string coord_port, std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  // register server
  bool status = register_server_with_coordinator(cluster_id, server_id, coord_ip, coord_port, port_no);
  // if server is already registered and alive, exit...
  if(!status) {
    std::cout << "Exiting...\n";
    return;
  }

  server->Wait();
}

int main(int argc, char** argv) {

  int cluster_id;
  int server_id;
  std::string coord_ip;
  std::string coord_port;
  std::string port = "3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
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
