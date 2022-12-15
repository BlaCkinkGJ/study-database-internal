use lsmtree::storage::{Storage, StorageConfig};

fn main() {
    let mut storage = Storage::new(StorageConfig::default_config()).unwrap();
    println!("get 0 {:?}", storage.get("0".as_bytes()));
    println!("get 1 {:?}", storage.get("1".as_bytes()));
    println!("get 2 {:?}", storage.get("2".as_bytes()));
    println!("set 0 {:?}", storage.set("0".as_bytes(), "0".as_bytes()));
    println!("set 1 {:?}", storage.set("1".as_bytes(), "1".as_bytes()));
    println!("set 2 {:?}", storage.set("2".as_bytes(), "2".as_bytes()));
    println!("get 0 {:?}", storage.get("0".as_bytes()));
    println!("get 1 {:?}", storage.get("1".as_bytes()));
    println!("get 2 {:?}", storage.get("2".as_bytes()));
    println!("del 1 {:?}", storage.delete("1".as_bytes()));
    println!("get 0 {:?}", storage.get("0".as_bytes()));
    println!("get 1 {:?}", storage.get("1".as_bytes()));
    println!("get 2 {:?}", storage.get("2".as_bytes()));
    //println!("sleep 10 seconds");
    //std::thread::sleep_ms(10_000);
    //storage.drop().unwrap();
}
