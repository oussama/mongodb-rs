
use bson::{Bson, Document, Array, Encoder, Decoder};


pub trait BsonDocument {
	
	fn insert_str(&mut self,key:&str,val:&String){}
	
}

impl BsonDocument for Document {
	fn insert_str(&mut self,key:&str,val:&String){
		self.insert(key.to_string(), Bson::String(val.clone()) );
	}
 }