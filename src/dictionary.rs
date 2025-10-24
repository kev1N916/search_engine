use std::collections::HashMap;
#[derive(Debug, Clone, PartialEq)]
pub struct Posting {
    pub doc_id: u32,
    pub positions: Vec<u32>,
}
impl Posting {
    pub fn new(doc_id: u32, positions: Vec<u32>) -> Self {
        Self { doc_id, positions }
    }
}
pub struct Term {
    pub term: String,
    pub posting: Posting,
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    size: u32,
    dictionary: HashMap<String, Vec<Posting>>,
}

impl Dictionary {
    pub fn new() -> Dictionary {
        return Dictionary {
            size: 0,
            dictionary: HashMap::new(),
        };
    }
    pub fn clear(& mut self){
        self.dictionary.clear();
    }
    pub fn get_size(& self)->u32{
        self.size
    }
    pub fn does_term_already_exist(&mut self, term: &str) -> bool {
        return self.dictionary.contains_key(term);
    }

    pub fn add_term_posting(&mut self, term: &str, posting: Vec<Posting>) {
        let posting_list = posting.clone();
        self.dictionary.insert(String::from(term), posting);
        for posting in posting_list {
            let posting_length = posting.positions.len() as u32;
            self.size = self.size + 4 + 4 * posting_length;
        }
        self.size = self.size + term.len() as u32;
    }

    pub fn get_postings(&self, term: &str) -> Option<Vec<Posting>> {
        if let Some(postings_list) = self.dictionary.get(term) {
            return Some(postings_list.clone());
        }
        None
    }

    pub fn add_term(&mut self, term: &str) {
        if !self.does_term_already_exist(term) {
            self.dictionary.insert(String::from(term), Vec::new());
            self.size = self.size + term.len() as u32
        }
    }

    pub fn append_to_term(&mut self, term: &str, posting: Posting) {
        if let Some(postings_list) = self.dictionary.get_mut(term) {
            let posting_length = posting.positions.len() as u32;
            postings_list.push(posting);
            self.size = self.size + 4 + 4 * posting_length;
        }
    }

    pub fn sort_terms(&self) -> Vec<String> {
        let mut sorted_terms: Vec<String> = Vec::new();
        for (term, _) in &self.dictionary {
            sorted_terms.push(term.to_string());
        }
        sorted_terms.sort();
        sorted_terms
    }
}

#[cfg(test)]
mod dictionary_tests {
    use super::*;

    #[test]
    fn test_does_term_already_exist_empty_dictionary() {
        let mut dict = Dictionary::new();
        assert!(!dict.does_term_already_exist("test"));
    }

    #[test]
    fn test_add_term() {
        let mut dict = Dictionary::new();
        dict.add_term("hello");

        assert!(dict.does_term_already_exist("hello"));
        assert_eq!(dict.dictionary.len(), 1);

        // Verify the term has an empty posting list
        let postings = dict.get_postings("hello");
        assert!(postings.is_some());
        assert_eq!(postings.unwrap().len(), 0);
    }

    #[test]
    fn test_add_term_posting() {
        let mut dict = Dictionary::new();
        let postings = vec![
            Posting::new(1, vec![5, 10, 15]),
            Posting::new(2, vec![3, 8]),
        ];

        dict.add_term_posting("rust", postings.clone());

        assert!(dict.does_term_already_exist("rust"));
        let retrieved_postings = dict.get_postings("rust");
        assert!(retrieved_postings.is_some());
        assert_eq!(retrieved_postings.unwrap(), postings);
    }

    #[test]
    fn test_get_postings_nonexistent_term() {
        let dict = Dictionary::new();
        let postings = dict.get_postings("nonexistent");
        assert!(postings.is_none());
    }

    #[test]
    fn test_append_to_term() {
        let mut dict = Dictionary::new();
        dict.add_term("programming");

        let posting1 = Posting::new(10, vec![2, 7, 12]);
        let posting2 = Posting::new(20, vec![4, 9]);

        dict.append_to_term("programming", posting1.clone());
        dict.append_to_term("programming", posting2.clone());

        let postings = dict.get_postings("programming").unwrap();
        assert_eq!(postings.len(), 2);
        assert_eq!(postings[0], posting1);
        assert_eq!(postings[1], posting2);
    }

    #[test]
    fn test_append_to_nonexistent_term() {
        let mut dict = Dictionary::new();
        let posting = Posting::new(1, vec![5]);

        // This should not panic, but also shouldn't add the posting
        dict.append_to_term("nonexistent", posting);

        // The term should still not exist
        assert!(!dict.does_term_already_exist("nonexistent"));
    }

    #[test]
    fn test_sort_terms_empty() {
        let dict = Dictionary::new();
        let sorted = dict.sort_terms();
        assert_eq!(sorted.len(), 0);
    }

    #[test]
    fn test_sort_terms_single_term() {
        let mut dict = Dictionary::new();
        dict.add_term("single");

        let sorted = dict.sort_terms();
        assert_eq!(sorted, vec!["single"]);
    }

    #[test]
    fn test_sort_terms_multiple_terms() {
        let mut dict = Dictionary::new();
        dict.add_term("zebra");
        dict.add_term("apple");
        dict.add_term("banana");
        dict.add_term("cherry");

        let sorted = dict.sort_terms();
        assert_eq!(sorted, vec!["apple", "banana", "cherry", "zebra"]);
    }

    #[test]
    fn test_sort_terms_case_sensitivity() {
        let mut dict = Dictionary::new();
        dict.add_term("Zebra");
        dict.add_term("apple");
        dict.add_term("Banana");

        let sorted = dict.sort_terms();
        // Note: uppercase letters come before lowercase in ASCII ordering
        assert_eq!(sorted, vec!["Banana", "Zebra", "apple"]);
    }

    #[test]
    fn test_overwrite_term_posting() {
        let mut dict = Dictionary::new();
        let initial_postings = vec![Posting::new(1, vec![1, 3])];
        let new_postings = vec![Posting::new(2, vec![2, 5, 8]), Posting::new(3, vec![3, 7])];

        dict.add_term_posting("test", initial_postings);
        dict.add_term_posting("test", new_postings.clone());

        let retrieved = dict.get_postings("test").unwrap();
        assert_eq!(retrieved, new_postings);
        assert_ne!(retrieved.len(), 1); // Should not be the initial posting
    }

    #[test]
    fn test_mixed_operations() {
        let mut dict = Dictionary::new();

        // Add some terms with postings
        dict.add_term_posting("search", vec![Posting::new(1, vec![5])]);
        dict.add_term_posting("engine", vec![Posting::new(2, vec![3])]);

        // Add empty term and then append to it
        dict.add_term("index");
        dict.append_to_term("index", Posting::new(3, vec![7]));

        // Verify all operations worked
        assert!(dict.does_term_already_exist("search"));
        assert!(dict.does_term_already_exist("engine"));
        assert!(dict.does_term_already_exist("index"));

        let search_postings = dict.get_postings("search").unwrap();
        assert_eq!(search_postings.len(), 1);
        assert_eq!(search_postings[0], Posting::new(1, vec![5]));

        let index_postings = dict.get_postings("index").unwrap();
        assert_eq!(index_postings.len(), 1);
        assert_eq!(index_postings[0], Posting::new(3, vec![7]));

        let sorted = dict.sort_terms();
        assert_eq!(sorted, vec!["engine", "index", "search"]);
    }

    #[test]
    fn test_thread_safety_simulation() {
        // This test simulates what might happen with concurrent access
        // Though we can't easily test true concurrency in unit tests
        let mut dict = Dictionary::new();
        dict.add_term_posting("concurrent", vec![Posting::new(1, vec![1])]);

        // Get multiple references to the same postings
        let postings1 = dict.get_postings("concurrent");
        let postings2 = dict.get_postings("concurrent");

        assert!(postings1.is_some());
        assert!(postings2.is_some());
        assert_eq!(postings1.unwrap(), postings2.unwrap());

        // Append to the term
        dict.append_to_term("concurrent", Posting::new(2, vec![2]));

        // Get postings again to verify the append worked
        let updated_postings = dict.get_postings("concurrent").unwrap();
        assert_eq!(updated_postings.len(), 2);
    }

    #[test]
    fn test_posting_with_multiple_positions() {
        let mut dict = Dictionary::new();

        // Test posting with multiple positions (typical for search index)
        let posting = Posting::new(42, vec![1, 5, 10, 15, 20]);
        dict.add_term("algorithm");
        dict.append_to_term("algorithm", posting.clone());

        let retrieved_postings = dict.get_postings("algorithm").unwrap();
        assert_eq!(retrieved_postings.len(), 1);
        assert_eq!(retrieved_postings[0].doc_id, 42);
        assert_eq!(retrieved_postings[0].positions, vec![1, 5, 10, 15, 20]);
    }

    #[test]
    fn test_posting_with_empty_positions() {
        let mut dict = Dictionary::new();

        // Test posting with no positions
        let posting = Posting::new(100, vec![]);
        dict.add_term("empty");
        dict.append_to_term("empty", posting.clone());

        let retrieved_postings = dict.get_postings("empty").unwrap();
        assert_eq!(retrieved_postings.len(), 1);
        assert_eq!(retrieved_postings[0].doc_id, 100);
        assert_eq!(retrieved_postings[0].positions.len(), 0);
    }
}

fn binary_strings_to_bytes_safe(
    binary_strings: Vec<String>,
) -> Result<Vec<u8>, std::num::ParseIntError> {
    binary_strings
        .iter()
        .map(|binary_str| u8::from_str_radix(binary_str, 2))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_vector() {
        let input = vec![];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_single_binary_string() {
        let input = vec!["10101010".to_string()];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, vec![170]);
    }

    #[test]
    fn test_multiple_binary_strings() {
        let input = vec![
            "10101010".to_string(), // 170
            "11110000".to_string(), // 240
            "00001111".to_string(), // 15
            "11111111".to_string(), // 255
            "00000000".to_string(), // 0
        ];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, vec![170, 240, 15, 255, 0]);
    }

    #[test]
    fn test_all_zeros() {
        let input = vec![
            "00000000".to_string(),
            "00000000".to_string(),
            "00000000".to_string(),
        ];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, vec![0, 0, 0]);
    }

    #[test]
    fn test_all_ones() {
        let input = vec!["11111111".to_string(), "11111111".to_string()];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, vec![255, 255]);
    }

    #[test]
    fn test_short_binary_strings() {
        let input = vec![
            "1".to_string(),       // 1
            "10".to_string(),      // 2
            "101".to_string(),     // 5
            "1111".to_string(),    // 15
            "1010101".to_string(), // 85
        ];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, vec![1, 2, 5, 15, 85]);
    }

    #[test]
    fn test_leading_zeros() {
        let input = vec![
            "00000001".to_string(), // 1
            "00000010".to_string(), // 2
            "00001000".to_string(), // 8
            "01010101".to_string(), // 85
        ];
        let result = binary_strings_to_bytes_safe(input).unwrap();
        assert_eq!(result, vec![1, 2, 8, 85]);
    }
}

pub fn vb_Encode(num: i32) -> Result<Vec<u8>, std::num::ParseIntError> {
    let binary_representation = binary_conversion(num);
    let mut binary_chunks = split_into_chunks_from_right(&binary_representation, 7);
    vb_encode(&mut binary_chunks);

    match binary_strings_to_bytes_safe(binary_chunks) {
        Ok(bytes) => {
            println!("Validated conversion: {:?}", bytes);
            return Ok(bytes);
        }
        Err(e) => {
            println!("Validation error: {}", e);
            return Err(e);
        }
    }
}

fn binary_conversion(mut num: i32) -> String {
    let mut binary = String::new();

    while num > 0 {
        binary = if num % 2 == 0 { "0" } else { "1" }.to_owned() + &binary;
        num /= 2;
    }

    binary
}

#[cfg(test)]
mod binary_conversion_tests {
    use super::*;

    #[test]
    fn test_zero() {
        let result = binary_conversion(1097);
        assert_eq!(result, "10001001001");
    }

    #[test]
    fn test_three() {
        let result = binary_conversion(3);
        assert_eq!(result, "11");
    }

    #[test]
    fn test_five() {
        let result = binary_conversion(5);
        assert_eq!(result, "101");
    }

    #[test]
    fn test_eight() {
        let result = binary_conversion(8);
        assert_eq!(result, "1000");
    }

    #[test]
    fn test_fifteen() {
        let result = binary_conversion(15);
        assert_eq!(result, "1111");
    }
}

fn split_into_chunks_from_right(s: &str, chunk_size: usize) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    let mut chunks = Vec::<String>::new();
    let mut current_chunk = String::new();

    for (i, &ch) in chars.iter().rev().enumerate() {
        if i > 0 && i % chunk_size == 0 {
            chunks.push(current_chunk.chars().rev().collect());
            current_chunk = String::new();
        }
        current_chunk.push(ch);
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk.chars().rev().collect());
    }

    chunks.reverse();

    if !chunks.is_empty() && chunks[0].len() < 7 {
        // You can't do chunks[0] = "0".to_string() + chunks[0]; because of Rust's ownership rules. Here's what's happening:
        // The Problem
        // When you write chunks[0] on the right side of the assignment, Rust tries to move the String out of the vector.
        // But you're also trying to assign back to chunks[0], which would require the original value to still be there.
        // This creates a conflict.
        for _i in 0..(7 - chunks[0].len()) {
            chunks[0] = "0".to_string() + &chunks[0];
        }
    }
    chunks
}
#[cfg(test)]
mod split_into_chunks_from_right_tests {

    use super::*;

    #[test]
    fn test_empty_string() {
        let result = split_into_chunks_from_right("", 7);
        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn test_exact_chunk_size() {
        let result = split_into_chunks_from_right("1010101", 7);
        assert_eq!(result, vec!["1010101".to_string()]);
    }

    #[test]
    fn test_multiple_exact_chunks() {
        let result = split_into_chunks_from_right("10101011100110", 7);
        assert_eq!(result, vec!["1010101".to_string(), "1100110".to_string()]);
    }

    #[test]
    fn test_partial_first_chunk() {
        let result = split_into_chunks_from_right("110101011100110", 7);
        assert_eq!(
            result,
            vec![
                "0000001".to_string(),
                "1010101".to_string(),
                "1100110".to_string()
            ]
        );
    }
}

fn vb_encode(number: &mut Vec<String>) {
    for i in 0..number.len() {
        if i == number.len() - 1 {
            number[i] = "1".to_owned() + &number[i]
        } else {
            number[i] = "0".to_owned() + &number[i]
        }
    }
}
#[cfg(test)]
mod vb_encode_tests {
    use super::*;

    #[test]
    fn test_empty_vector() {
        let mut input = vec![];
        vb_encode(&mut input);
        assert_eq!(input, Vec::<String>::new());
    }

    #[test]
    fn test_single_element() {
        let mut input = vec!["1010101".to_string()];
        vb_encode(&mut input);
        assert_eq!(input, vec!["11010101".to_string()]);
    }

    #[test]
    fn test_two_elements() {
        let mut input = vec!["1010101".to_string(), "0110011".to_string()];
        vb_encode(&mut input);
        assert_eq!(input, vec!["01010101".to_string(), "10110011".to_string()]);
    }

    #[test]
    fn test_multiple_elements() {
        let mut input = vec![
            "0000001".to_string(),
            "0000010".to_string(),
            "0000011".to_string(),
            "0000100".to_string(),
            "0000101".to_string(),
        ];
        vb_encode(&mut input);
        assert_eq!(
            input,
            vec![
                "00000001".to_string(),
                "00000010".to_string(),
                "00000011".to_string(),
                "00000100".to_string(),
                "10000101".to_string()
            ]
        );
    }
    #[test]
    fn test_mixed_length_strings() {
        let mut input = vec![
            "1".to_string(),
            "10".to_string(),
            "101".to_string(),
            "1010".to_string(),
        ];
        vb_encode(&mut input);
        assert_eq!(
            input,
            vec![
                "01".to_string(),
                "010".to_string(),
                "0101".to_string(),
                "11010".to_string()
            ]
        );
    }
}
