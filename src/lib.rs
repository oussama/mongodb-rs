#![crate_name = "mongodb"]


extern crate byteorder;
extern crate bson;

pub mod msg;
pub mod utils;

use self::msg::*;
use self::utils::*;
use self::bson::{Bson, Document, Array, Encoder, Decoder};
use std::net::TcpStream;
use std::io::{Write,Read};
use std::cell::RefCell;
use std::sync::Arc;
use std::collections::BTreeMap;




pub trait Connection:Read+Write{}
impl<T: Read + Write> Connection for T {}

pub struct Client<T : Connection> {	
	pub connection:Arc<RefCell<T>>,	
	pub dbs:BTreeMap<String, Arc<RefCell<Database<T>>>>,
}

impl<T: Connection> Client<T> {
		
	pub fn new(address:&str) -> Option<Client<TcpStream>> {
		match TcpStream::connect(address) {
			Ok(tcp_stream) => {
				Some(Client{
					connection:Arc::new(RefCell::new(tcp_stream)),	
					dbs:BTreeMap::new()
				})
			}
			Err(error) => None
		}
	}
	
	pub fn new_with(connection:T)-> Client<T>{
		Client{
			connection:Arc::new(RefCell::new(connection)),	
			dbs:BTreeMap::new()
		}
	}
	
	pub fn db(&self,name:&str)-> Database<T> {
		Database {
			connection:self.connection.clone(),
			name:name.to_string()
		}
	}
	
	
	
	
} 

pub struct Database<T : Connection> {
	connection:Arc<RefCell<T>>,
	name:String
}

impl<T : Connection> Database<T> {
	
	
}

impl<T : Connection> Database<T> {
	
	#[inline]
	fn send<M:Message>(&self,mut msg:M)  {
		msg.encode(&mut *self.connection.borrow_mut());
	}
	
	#[inline]
	fn recv(&self) -> OpReply {
		OpReply::decode(&mut *self.connection.borrow_mut())
	}
	

	
	pub fn coll(&self,name:&str) -> Collection<T>{
		Collection{
			connection:self.connection.clone(),
			db_name:self.name.clone(),
			name:name.to_string(),
			name_space:self.name.clone()+"."+name.clone()
		}
	}
		
}

pub struct Collection<T : Connection> {
	
	connection:Arc<RefCell<T>>,
	db_name:String,
	name:String,
	name_space:String
}

impl<T: Connection> Collection<T > {	
	
	#[inline]
	fn send<M:Message>(&self,mut msg:M)  {
		msg.encode(&mut *self.connection.borrow_mut());
	}
	
	#[inline]
	fn recv(&self) -> OpReply {
		OpReply::decode(&mut *self.connection.borrow_mut())
	}
	
	pub fn command(&self,doc:&Document) -> OpReply {
		let mut msg = OpQuery::new(&self.name_space,doc,None,1);
		self.send(msg);
		self.recv()
	}
	
	pub fn insert(&self,doc:&mut Document){
		let mut docs = Vec::new();
		docs.push(&*doc);
		
		let mut msg = OpInsert::new(&self.name_space,&docs);
		self.send(msg);
	}
	
	pub fn insert_bulk(&self,docs:&Vec<&Document>){
		let mut msg = OpInsert::new(&self.name_space,docs);
		self.send(msg);
	}
	
	pub fn find_one(&self,selector:&Document)-> Option<Document> {
		let mut msg = OpQuery::new(&self.name_space,selector,None,1);
		self.send(msg);
		let reply = OpReply::decode(&mut *self.connection.borrow_mut());
		if reply.docs().len() == 0 {
			None
		}else{
			Some(reply.docs()[0].clone())
		}
	}
	
	
	// TODO: check, fix and create better return
	pub fn find_and_modify(&self,query:Bson,update:Bson)-> Option<Document> {
		let mut doc = Document::new();
		doc.insert_str("findAndModify", &self.name);
    	doc.insert("query".to_string(), query );
    	doc.insert("update".to_string(), update );
		let reply = self.command(&doc);

		if reply.docs().len() == 0 {
			None
		}else{
			Some(reply.docs()[0].clone())
		}
	}
	
	pub fn count(&self)-> i32 {
		let mut doc = Document::new();
    	doc.insert_str("count", &self.name);
		let reply = self.command(&doc);
		match *reply.docs()[0].get("n").unwrap() {
			Bson::I32(n) => n,
			_ => -1	
		}
	}
}


