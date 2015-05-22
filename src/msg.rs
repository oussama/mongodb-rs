


use byteorder::{WriteBytesExt,ReadBytesExt,LittleEndian};
use bson::{Bson, Document, Array, Encoder, Decoder};
use std::io::{Write,Read};
use std::ffi::CString;


pub trait Message {	
	fn encode(&mut self,&mut Write){
		panic!("Sorry, not implemented yet");
	}
	fn len(&self)->i32 {0i32}
}

trait WriteMessage {
	fn write_message<T:Message>(&mut self,msg:&mut T){}
}
impl WriteMessage for Write {
	fn write_message<T:Message>(&mut self,msg:&mut T){
		msg.encode(self);
	}
}

#[derive(Clone)]
pub enum OpCode {
    OP_REPLY = 1 ,
    OP_UPDATE = 2001,
    OP_INSERT = 2002,
    RESERVED = 2003,
    OP_QUERY = 2004,
    OP_GET_MORE = 2005,
    OP_DELETE = 2006,
    OP_KILL_CURSORS = 2007,
}
impl OpCode {
	pub fn from_i32(n:i32)-> OpCode {
		match (n) {
		        1 => OpCode::OP_REPLY,
		        2001 => OpCode::OP_UPDATE,
		        _ => OpCode::OP_KILL_CURSORS
		}
	}
}

#[derive(Clone)]
pub struct MsgHeader {
    len : i32,      
    id : i32,      
    resp_to : i32,  
    opcode : OpCode,
}
impl MsgHeader {
	pub fn new(opcode:OpCode)->MsgHeader {
		MsgHeader {
			len:0,
			id:0,
			resp_to:0,
			opcode:opcode,
		}
	}
	pub fn write_to(&self, buf:&mut Write){
		let _ = buf.write_i32::<LittleEndian>(self.len); 
		let _ = buf.write_i32::<LittleEndian>(self.id); 
		let _ = buf.write_i32::<LittleEndian>(self.resp_to); 
		let _ = buf.write_i32::<LittleEndian>(self.opcode.clone() as i32); 
	}
	pub fn set_id(&mut self,id:i32){
		self.id = id;
	}
	pub fn get_id(&mut self)->i32{
		self.id
	}
	pub fn set_len(&mut self,val:i32){
		self.len = val;
	}
	pub fn decode(buf:&mut Read)-> MsgHeader
	{
		MsgHeader{
			len: buf.read_i32::<LittleEndian>().unwrap(),
			id: buf.read_i32::<LittleEndian>().unwrap(),
			resp_to: buf.read_i32::<LittleEndian>().unwrap(),
			opcode: {
				let opcode = buf.read_i32::<LittleEndian>().unwrap();
				OpCode::from_i32(opcode)
				},
		}
	}
}

pub struct OpGetMore {                    
        header : MsgHeader,
        reserved_bits : i32,
        name_space : CString,
        nret : i32,
        cursor_id : i64,
}
impl OpGetMore {
	pub fn new(name_space:&str,cursor_id:i64,nret:i32) ->OpGetMore {	 
		let mut msg = OpGetMore {
			header: MsgHeader::new(OpCode::OP_GET_MORE),
	        reserved_bits : 0,
	        name_space :CString::new(name_space).unwrap(),
	        nret : nret,
	        cursor_id : cursor_id,
        };
		let msg_len = msg.len();
		msg.header.set_len(msg_len);
		msg
	}
}
impl  Message for OpGetMore {
	fn encode(&mut self,buf:&mut Write){
		self.header.write_to(buf);
        let _ = buf.write_i32::<LittleEndian>(self.reserved_bits); 
        let _ = buf.write(self.name_space.as_bytes()); 
        let _ = buf.write(&[0u8]); // null-terminate name
        let _ = buf.write_i32::<LittleEndian>(self.nret); 
        let _ = buf.write_i64::<LittleEndian>(self.cursor_id); 
	}
	fn len(&self)->i32{
		(16+17+self.name_space.as_bytes().len()) as i32
	}
}
                

pub struct OpInsert {                              // gets no response
        header : MsgHeader,
        flags : i32,
        name_space : CString,
        docs : Vec<u8>,
}
impl OpInsert {
	pub fn new(name_space:&str,docs:&Vec<&Document>) ->OpInsert {
		let mut docs_bytes = Vec::<u8>::new();
        {
	       let mut encoder = Encoder::new(&mut docs_bytes);
		   for doc in docs {
		   		let _ = encoder.encode_document(doc);
		   }
	    }
			 
		let mut msg = OpInsert {
			header: MsgHeader::new(OpCode::OP_INSERT),
	        flags : 0,
	        name_space :CString::new(name_space).unwrap(),
	        docs : docs_bytes,
        };
		let msg_len = msg.len();
		msg.header.set_len(msg_len);
		msg
	}
	pub fn set_flags(&mut self,flags:i32){
		self.flags = flags;
	}
}
impl  Message for OpInsert {
	fn encode(&mut self,buf:&mut Write){
		self.header.write_to(buf);
        let _ = buf.write_i32::<LittleEndian>(self.flags); 
        let _ = buf.write(self.name_space.as_bytes()); 
        let _ = buf.write(&[0u8]); // null-terminate name
        let _ = buf.write(&self.docs);
	}
	fn len(&self)->i32{
		(16+9+self.name_space.as_bytes().len()+self.docs.len()) as i32
	}
}



pub struct OpUpdate {  
	header : MsgHeader,
    reserved_bits : i32,
    name_space : CString,
    flags : i32,
    selector : Vec<u8>,
    update_ops : Vec<u8>,
}
impl OpUpdate {
	
	pub fn new(name_space:&str,selector:&Document,update_ops:&Document) ->OpUpdate {
		let mut selector_bytes = Vec::<u8>::new();
        {
	       let mut encoder = Encoder::new(&mut selector_bytes);
		   let _ = encoder.encode_document(&selector);
	    }
        let mut update_ops_bytes = Vec::<u8>::new();
        {
	       let mut encoder = Encoder::new(&mut update_ops_bytes);
		   let _ = encoder.encode_document(&update_ops);
	    }
			 
		let mut msg = OpUpdate {
			header: MsgHeader::new(OpCode::OP_UPDATE),
	        reserved_bits : 0,
	        name_space :CString::new(name_space).unwrap(),
	        flags : 0,
	        selector : selector_bytes,
	        update_ops : update_ops_bytes,
        };
		let msg_len = msg.len();
		msg.header.set_len(msg_len);
		msg
	}
	
	pub fn set_flags(&mut self,flags:i32){
		self.flags = flags;
	}
}
impl  Message for OpUpdate {
	fn encode(&mut self,buf:&mut Write){
		self.header.write_to(buf);
        let _ = buf.write_i32::<LittleEndian>(self.reserved_bits); 
        let _ = buf.write(self.name_space.as_bytes()); 
        let _ = buf.write(&[0u8]); // null-terminate name
        let _ = buf.write_i32::<LittleEndian>(self.flags); 
        let _ = buf.write(&self.selector);
		let _ = buf.write(&self.update_ops);
	}
	fn len(&self)->i32{
		(16+9+self.name_space.as_bytes().len()+self.selector.len()+self.update_ops.len()) as i32
	}
}



pub struct OpDelete {                              // gets no response
    header : MsgHeader,
    reserved_bits : i32,
    name_space : CString,
    flags : i32,
    selector : Vec<u8>,
}
impl OpDelete {
	pub fn new(name_space:&str,selector:&Document) ->OpDelete {
		let mut selector_bytes = Vec::<u8>::new();
        {
	       let mut encoder = Encoder::new(&mut selector_bytes);
		   let _ = encoder.encode_document(&selector);
	    }
		let mut msg = OpDelete {
			header: MsgHeader::new(OpCode::OP_DELETE),
	        reserved_bits : 0,
	        name_space :CString::new(name_space).unwrap(),
	        flags : 0,
	        selector : selector_bytes,
        };
		let msg_len = msg.len();
		msg.header.set_len(msg_len);
		msg
	}
}
impl Message for OpDelete {
	fn encode(&mut self,buf:&mut Write){
		self.header.write_to(buf);
        let _ = buf.write_i32::<LittleEndian>(self.reserved_bits); 
        let _ = buf.write(self.name_space.as_bytes()); 
        let _ = buf.write(&[0u8]); // null-terminate name
        let _ = buf.write_i32::<LittleEndian>(self.flags); 
        let _ = buf.write(&self.selector);
	}
	fn len(&self)->i32{
		(16+9+self.name_space.as_bytes().len()+self.selector.len()) as i32
	}
}



pub struct OpKillCursors {
        header : MsgHeader,
        reserved_bits : i32,
        ncursor_ids : i32,
        cursor_ids : Vec<i64>
}
impl OpKillCursors {
	pub fn new(cursor_ids : Vec<i64>) ->OpKillCursors {
		let len = 16 + 4*3 + 8*cursor_ids.len();
		let mut msg = OpKillCursors {
			header: MsgHeader::new(OpCode::OP_KILL_CURSORS),
	        reserved_bits : 0,
	        ncursor_ids :cursor_ids.len() as i32,
	        cursor_ids : cursor_ids,
        };
		msg.header.set_len(len as i32);
		msg
	}
	pub fn set_reserved_bits(&mut self,reserved_bits:i32){
		self.reserved_bits =reserved_bits;
	}
}
impl Message for OpKillCursors {
	fn encode(&mut self,buf:&mut Write){
		self.header.write_to(buf);
        let _ = buf.write_i32::<LittleEndian>(self.reserved_bits); 
        let _ = buf.write_i32::<LittleEndian>(self.ncursor_ids); 
        for id in &self.cursor_ids {
        	let _ = buf.write_i64::<LittleEndian>(*id);
        };
	}
}




pub struct OpQuery {                         
    header : MsgHeader,
    flags : i32,
    name_space : CString,
    nskip : i32,
    nret : i32,
    query : Vec<u8>,
    ret_field_selector : Option<Vec<u8>>,
}
impl OpQuery {
	pub fn new(name_space:&str,query:&Document,ret_field_selector: Option<&Document>) ->OpQuery {
		let mut query_bytes = Vec::<u8>::new();
        {
	       let mut encoder = Encoder::new(&mut query_bytes);
		   let _ = encoder.encode_document(&query);
	    }
        let ret_field_selector_bytes  = match ret_field_selector {
        	Some(selector) => {
        		let mut bytes = Vec::<u8>::new();
		        {
			       let mut encoder = Encoder::new(&mut bytes);
				   let _ = encoder.encode_document(&selector);
			    }
        		Some(bytes)
        	}
        	None => None,
        };
		let mut msg = OpQuery {
			header: MsgHeader::new(OpCode::OP_QUERY),
	        flags : 0,
	        name_space :CString::new(name_space).unwrap(),
	        nskip : 0,
        	nret : 0,
	        query : query_bytes,
	        ret_field_selector : ret_field_selector_bytes,
        };
		
		let msg_len = msg.len();
		msg.header.set_len(msg_len);
		msg
	}
	pub fn set_flags(&mut self,flags:i32){
		self.flags = flags;
	}
}
impl  Message for OpQuery {
	fn encode(&mut self,buf:&mut Write){
		self.header.write_to(buf);
        let _ = buf.write_i32::<LittleEndian>(self.flags); 
        let _ = buf.write(self.name_space.as_bytes()); 
        let _ = buf.write(&[0u8]); // null-terminate name
        let _ = buf.write_i32::<LittleEndian>(self.nskip); 
        let _ = buf.write_i32::<LittleEndian>(self.nret); 
        let _ = buf.write(&self.query);
	    match &self.ret_field_selector {
	        &Some(ref selector) => { 
	        		let _ = buf.write(&selector); 
	        	}
			&None => (),
	    };
	}
	fn len(&self)->i32{
		let rfs_len = match &self.ret_field_selector{
			&Some(ref selector) => selector.len(),
			&None => 0
		};
		(16+13+self.name_space.as_bytes().len()+self.query.len()+rfs_len) as i32
	}
}



pub struct OpReply {
    header : MsgHeader,
    flags : i32,
    cursor_id : i64,
    start : i32,
    nret : i32,
    pub docs : Vec<Document>,
}
impl OpReply {
	pub fn decode(buf:&mut Read)-> OpReply{
		let header = MsgHeader::decode(buf);
		let mut docs_count;
		OpReply {
			header: header,
			flags: buf.read_i32::<LittleEndian>().unwrap(),
			cursor_id: buf.read_i64::<LittleEndian>().unwrap(),
			start: buf.read_i32::<LittleEndian>().unwrap(),
			nret: {
				docs_count = buf.read_i32::<LittleEndian>().unwrap();
				docs_count.clone()
				},
			docs:{
				let mut docs = Vec::<Document>::new();
				let mut decoder = Decoder::new(buf);
				for _ in 0..docs_count {
					docs.push(decoder.decode_document().unwrap());
				}
				docs
			},
		}
	}
}