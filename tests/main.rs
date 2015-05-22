extern crate mongodb;
extern crate bson;
use std::cell::RefCell;

use std::net::TcpStream;
use std::io::{Write,Read};

use mongodb::msg::*;

use self::bson::{Bson, Document, Array, Encoder, Decoder};
use std::ffi::CString;

#[test]
fn main() {
    println!("Hsssllo, world!");
    let mut stream = TcpStream::connect("127.0.0.1:27017").unwrap();
	
	let header = MsgHeader::new(OpCode::OP_QUERY);
	
	
	let mut doc = Document::new();
    doc.insert("foo".to_string(), Bson::String("bar".to_string()));
	doc.insert("_id".to_string(), Bson::ObjectId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]));
   /* let mut arr = Array::new();
    arr.push(Bson::String("blajh".to_string()));

    doc.insert("array".to_string(), Bson::Array(arr));
	*/
	let mut msg = OpQuery::new("db_name.collection_name",&doc,None);

    msg.encode(&mut stream);
    print!("sent {}","");
	//let mut r = Vec::new();
    //let _ = stream.read_to_end(&mut r); // ignoress h
    let reply = OpReply::decode(&mut stream);
    //let mut m = [0u8];
    //let _ = stream.read(&mut m);


	print!("end {} , {}", reply.docs.len(),"redply.nret");

}	




fn connect(url:String){
	
}



struct Database {
	
	name:String
}

impl Database {
	
	pub fn coll(self,name:String) -> Collection{
		Collection{db:RefCell::new(self),name:name}
	}
	
	
}


struct Collection {
	db:RefCell<Database>,
	name:String
}


impl Collection {
	
	pub fn query(&self){
		
		
	}
	
	
}