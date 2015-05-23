#![crate_name = "mongodb"]


extern crate byteorder;
extern crate bson;

pub mod msg;

use self::msg::*;
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
	
	
	pub fn insert(&self,doc:&mut Document){
		//doc.insert("_id".to_string(), Bson::ObjectId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]));
		let mut docs = Vec::new();
		docs.push(&*doc);
		
		let mut msg = OpInsert::new(&self.name_space,&docs);
		msg.encode(&mut *self.connection.borrow_mut());
	}
	
	pub fn insert_multiple(&self,docs:&Vec<&Document>){
		let mut msg = OpInsert::new(&self.name_space,docs);
		msg.encode(&mut *self.connection.borrow_mut());
	}
	
	pub fn count(&self)-> i32 {
		let mut doc = Document::new();
    	doc.insert("count".to_string(), Bson::String(self.name.clone()) );
		let reply = run_command(&mut *self.connection.borrow_mut(),self.db_name.clone(),&doc);
		match *reply.docs()[0].get("n").unwrap() {
			Bson::I32(n) => n,
			_ => -1	
		}
	}
}


fn run_command<T:Connection>(connection:&mut T,db_name:String,doc:&Document) -> OpReply {
		let name_space = db_name+".$cmd";
		let mut msg = OpQuery::new(&*name_space,&doc,None,1);
		msg.encode(connection);
		OpReply::decode(connection)
}


