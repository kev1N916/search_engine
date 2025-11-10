use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, Read},
    path::Path,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    dictionary::{Posting, Term},
    in_memory_dict::map_in_memory_dict::MapInMemoryDict,
    indexer::spimi::Spmi,
    my_bk_tree::BkTree,
    query_parser::tokenizer::SearchTokenizer,
};
use bzip2::read::BzDecoder;
use regex::Regex;
use serde::{Deserialize, Serialize};

// Define the structure matching your JSON format
#[derive(Debug, Deserialize, Serialize)]
struct WikiArticle {
    url: String,
    text: Vec<Vec<String>>,
    id: String,
    title: String,
}
pub struct IndexMetadata {
    bk_tree: BkTree,
    in_memory_dictionary: MapInMemoryDict,
    term_to_id_map: HashMap<String, u32>,
}

impl IndexMetadata {
    pub fn new() -> Self {
        Self {
            bk_tree: BkTree::new(),
            in_memory_dictionary: MapInMemoryDict::new(),
            term_to_id_map: HashMap::new(),
        }
    }
    pub fn add_term(term: String) {}
}
pub struct Indexer {
    doc_id: u32,
    index_directory_path: String,
    search_tokenizer: SearchTokenizer,
}

fn extract_plaintext(text: &Vec<Vec<String>>) -> String {
    // Join all paragraphs and sentences
    let full_text = text
        .iter()
        .map(|paragraph| paragraph.join(""))
        .collect::<Vec<String>>()
        .join("\n\n"); // Separate paragraphs with double newline

    // Remove all HTML/XML tags using regex
    let tag_regex = Regex::new(r"<[^>]*>").unwrap();
    tag_regex.replace_all(&full_text, "").to_string()
}
impl Indexer {
    pub fn new() -> Result<Self, std::io::Error> {
        let search_tokenizer = SearchTokenizer::new()?;
        Ok(Self {
            doc_id: 0,
            // term_sender: tx,
            // term_receiver: rx,
            index_directory_path: String::new(),
            search_tokenizer: search_tokenizer,
        })
    }

    fn read_bz2_file(
        &mut self,
        path: &Path,
        tx:&mpsc::Sender<Term>,
    ) -> Result<Vec<WikiArticle>, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let decoder = BzDecoder::new(file);
        let reader = BufReader::new(decoder);

        // Create a streaming deserializer
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<WikiArticle>();

        let mut articles = Vec::new();

        for (i, result) in stream.enumerate() {
            match result {
                Ok(article) => {
                    let plain_text = extract_plaintext(&article.text);
                    let tokens = self.search_tokenizer.tokenize(plain_text);

                    articles.push(article);
                    let mut doc_postings: HashMap<String, Vec<u32>> = HashMap::new();
                    for token in &tokens {
                        doc_postings
                            .entry(token.word.clone())
                            .or_insert(Vec::new())
                            .push(token.position);
                    }
                    for (key, value) in doc_postings {
                        self.doc_id += 1;
                        let term = Term {
                            posting: Posting {
                                doc_id: self.doc_id,
                                positions: value,
                            },
                            term: key,
                        };
                        tx.send(term).unwrap();
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing object {}: {}", i + 1, e);
                    // Optionally break or continue based on your needs
                }
            }
        }

        Ok(articles)
    }

    fn scan_index_directory(&self, directory: &str) -> Result<Vec<File>, io::Error> {
        let mut file_handles = Vec::new();

        let entries = fs::read_dir(directory)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension == "txt" {
                        let file = File::open(&path)?;
                        file_handles.push(file);
                    }
                }
            }
        }

        Ok(file_handles)
    }

    fn process_directory(&mut self, dir_path: &Path,tx:&mpsc::Sender<Term>,) -> Result<u32, Box<dyn std::error::Error>> {
        let mut number_of_articles: u32 = 0;

        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // Recursively process subdirectories
                number_of_articles += self.process_directory(&path,&tx)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("bz2") {
                println!("Processing: {:?}", path);
                let pages = self.read_bz2_file(&path,tx)?;

                for page in pages {
                    number_of_articles += 1;
                    println!("ID: {}, Title: {}", page.id, page.title);
                    // println!("Content: {}",page.text)
                    // Process each page as needed
                }
            }
        }

        Ok(number_of_articles)
    }

    pub fn set_index_directory(&mut self, index_directory_path: String) {
        self.index_directory_path = index_directory_path;
    }
    pub fn index(&mut self) -> io::Result<()> {
        let mut spmi = Spmi::new();
        let (tx, rx) = mpsc::channel::<Term>();

        let handle = std::thread::spawn(move || {
            let _ = spmi.single_pass_in_memory_indexing(rx); // Use the moved variable
        });

        let wiki_dir = Path::new("enwiki-20171001-pages-meta-current-withlinks-processed");
        let _=self.process_directory(wiki_dir,&tx);
        drop(tx);
        handle.join().unwrap();
        Ok(())
    }
}
