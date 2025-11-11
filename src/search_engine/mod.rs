use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
    path::Path,
};

use crate::{
    dictionary::{ Posting},
    indexer::indexer::Indexer,
    query_parser::tokenizer::SearchTokenizer,
};

pub struct QueryResult {
    doc_ids: Vec<u32>,
}

pub struct SearchEngine {
    // query_processor:QueryPro
    query_parser: SearchTokenizer,
    indexer: Indexer,
    index_directory_path: String,
}

impl SearchEngine {
    pub fn new(index_directory_path: String) -> Result<Self, Error> {
        let path = Path::new(&index_directory_path);
        if !path.exists() || !path.is_dir() {
            return Err(Error::new(ErrorKind::Other, "index directory path does not exist, please initialize it "));
        }
        let query_parser=SearchTokenizer::new()?;
        let mut indexer=Indexer::new(query_parser.clone())?;
        indexer.set_index_directory(index_directory_path.clone());

        Ok(Self {
            index_directory_path:index_directory_path,
            query_parser:query_parser,
            indexer:indexer,
        })
    }

    pub fn build_index(&mut self) -> Result<(), io::Error> {
        self.indexer.index()?;
        Ok(())
    }

    pub fn set_index_directory_path(& mut self,index_directory_path: String){
        self.index_directory_path=index_directory_path;
    }

    // pub fn get_postings_from_index(
    //     &self,
    //     posting_offsets: &[PostingOffset],
    // ) -> Result<HashMap<String, (u16, Vec<Posting>)>, io::Error> {

    // }

    fn get_scores_for_docs(&self,no_of_docs:u32, query_terms: &HashMap<String, (u16, Vec<Posting>)>) -> Vec<f32> {
        let scores: Vec<f32> = vec![0.0; no_of_docs as usize];
        // for (_, (_, posting_list)) in query_terms {
        //     let df: f32 = get_document_frequency(posting_list);
        //     for posting in posting_list {
        //         let tf = get_term_frequency(posting);
        //         let idf = get_inverse_document_frequency(df, self.no_of_docs);
        //         let weight = get_tf_idf_weight(tf, idf);
        //         scores[posting.doc_id as usize] = scores[posting.doc_id as usize] + weight;
        //     }
        // }
        scores
    }
    pub fn handle_query(&self, query: String) -> Result<(), io::Error> {
        let token_query_result = self.query_parser.tokenize_query(query);
        if token_query_result.is_err() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "error"));
        }

        let tokens = token_query_result.unwrap();

        // let mut unigram_posting_offsets: Vec<PostingOffset> = Vec::new();
        // let mut bigram_posting_offsets: Vec<PostingOffset> = Vec::new();

        // let mut unigram_words = Vec::new();
        // let mut bigram_words = Vec::new();

        // for token in &tokens.unigram {
        //     // let posting_offset = self.in_memory_index.lock().unwrap().find(&token.word);
        //     let word = &token.word;
        //     unigram_words.push(word.clone());
        //     // unigram_posting_offsets.push(PostingOffset {
        //     //     word: word.clone(),
        //     //     posting_offset: posting_offset,
        //     // });
        // }

        // for token in &tokens.bigram {
        //     // let posting_offset = self.in_memory_index.lock().unwrap().find(&token.word);
        //     let word = &token.word;
        //     bigram_words.push(word.clone());
        //     // bigram_posting_offsets.push(PostingOffset {
        //     //     word: word.clone(),
        //     //     posting_offset: posting_offset,
        //     // });
        // }

        // let unigram_postings_list: HashMap<String, (u16, Vec<Posting>)> =
        //     self.get_postings_from_index(&unigram_posting_offsets)?;
        // let bigram_postings_list: HashMap<String, (u16, Vec<Posting>)> =
        //     self.get_postings_from_index(&bigram_posting_offsets)?;

        // let unigram_postings_offsets_copy: Vec<PostingOffset> = unigram_posting_offsets.clone();
        // let unigram_postings_list_copy: HashMap<String, (u16, Vec<Posting>)> =
        //     unigram_postings_list.clone();

        // let unigram_scores = self.get_scores_for_docs(&unigram_postings_list);
        // let unigram_scores_copy = unigram_scores.clone();

        // let bigram_scores = self.get_scores_for_docs(&bigram_postings_list);

        // // Create channels for communication with each thread
        // let (tx1, rx1) = mpsc::channel();
        // let (tx2, rx2) = mpsc::channel();
        // let (tx3, rx3) = mpsc::channel();

        // // Thread 1: Top-K scoring with BinaryHeap
        // let handle1 = thread::spawn(move || {
        //     let docs =
        //         find_documents_optimized(unigram_words, &unigram_postings_list, true);

        //     let mut max_heap: BinaryHeap<ScoredDoc> = BinaryHeap::new();
        //     for doc_id in docs {
        //         max_heap.push(ScoredDoc {
        //             doc_id: doc_id,
        //             score: unigram_scores[(doc_id - 1) as usize],
        //         });
        //     }

        //     let k = 2;
        //     let mut top_k = Vec::new();
        //     for _ in 0..k.min(max_heap.len()) {
        //         if let Some(item) = max_heap.pop() {
        //             top_k.push((item.doc_id, item.score));
        //         }
        //     }

        //     tx1.send(top_k).unwrap();
        // });

        // // Thread 2: Simple sorting approach (alternative ranking)
        // let handle2 = thread::spawn(move || {
        //     let docs =
        //         find_documents_optimized(bigram_words, &bigram_postings_list, false);

        //     let mut max_heap: BinaryHeap<ScoredDoc> = BinaryHeap::new();
        //     for doc_id in docs {
        //         max_heap.push(ScoredDoc {
        //             doc_id: doc_id,
        //             score: bigram_scores[(doc_id - 1) as usize],
        //         });
        //     }

        //     let k = 2;
        //     let mut top_k = Vec::new();
        //     for _ in 0..k.min(max_heap.len()) {
        //         if let Some(item) = max_heap.pop() {
        //             top_k.push((item.doc_id, item.score));
        //         }
        //     }

        //     tx2.send(top_k).unwrap();
        // });

        // // Thread 3: Statistical analysis of scores
        // let handle3 = thread::spawn(move || {
        //     // let docs = find_documents_optimized(
        //     //     &unigram_postings_offsets_copy,
        //     //     &unigram_postings_list_copy,
        //     //     false,
        //     // );

        //     let mut max_heap: BinaryHeap<ScoredDoc> = BinaryHeap::new();
        //     // for doc_id in docs {
        //     //     max_heap.push(ScoredDoc {
        //     //         doc_id: doc_id,
        //     //         score: unigram_scores_copy[(doc_id - 1) as usize],
        //     //     });
        //     // }

        //     let k = 2;
        //     let mut top_k = Vec::new();
        //     for _ in 0..k.min(max_heap.len()) {
        //         if let Some(item) = max_heap.pop() {
        //             top_k.push((item.doc_id, item.score));
        //         }
        //     }

        //     tx3.send(top_k).unwrap();
        // });

        // // Wait for all threads to complete and collect results
        // let mut phrase_docs: Vec<(u32, f32)> = rx1.recv().unwrap();
        // let mut bigram_docs: Vec<(u32, f32)> = rx2.recv().unwrap();
        // let mut unigram_docs: Vec<(u32, f32)> = rx3.recv().unwrap();

        // let mut query_result: Vec<(u32, f32)> = Vec::new();
        // query_result.append(&mut phrase_docs);
        // query_result.append(&mut bigram_docs);
        // query_result.append(&mut unigram_docs);

        // // Optional: Join handles to ensure threads completed properly
        // handle1.join().unwrap();
        // handle2.join().unwrap();
        // handle3.join().unwrap();

        Ok(())
    }
}
