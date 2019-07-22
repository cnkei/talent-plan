use bson;
use serde::{Serialize, Deserialize};

const DB_FILE: &'static str = "kvstore.dat";

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String),
    Rm,
}

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    timestamp: u32,
    key: String,
    command: Command,
}

impl From<Entry> for Vec<u8> {
    fn from(entry: Entry) -> Self {
        let mut buf = Vec::new();
        buf.extend(0u32.to_le_bytes().into_iter());
        buf.extend(entry.timestamp.to_le_bytes().into_iter());
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut cmds = vec![
            Command::Set("text1".to_owned()),
            Command::Rm,
        ];
        let buf = cmds.iter().map(|c| {
            let document = bson::to_bson(c).unwrap();
            let mut buf = vec![];
            println!("{:?} {:?}", c, document);
            if let bson::Bson::Document(document) = document {
                bson::encode_document(&mut buf, &document).unwrap();
            }
            buf
        }).fold(Vec::new(), |mut acc, x| { acc.extend(x.into_iter()); acc });
        println!("{:?}", buf);
    }
}