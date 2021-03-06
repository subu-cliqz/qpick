extern crate fst;
extern crate byteorder;
extern crate serde_json;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::collections::HashSet;

use byteorder::{ByteOrder, LittleEndian};
use fst::Map;
use std::io::SeekFrom;

use fst::Error;

pub mod util;
pub mod config;
pub mod ngrams;
pub mod merge;
pub mod shard;
pub mod builder;
pub mod stopwords;

extern crate scoped_threadpool;
use scoped_threadpool::Pool;

macro_rules! make_static_var_and_getter {
    ($fn_name:ident, $var_name:ident, $t:ty) => (
    static mut $var_name: Option<$t> = None;
    #[inline]
    fn $fn_name() -> &'static $t {
        unsafe {
            match $var_name {
                Some(ref n) => n,
                None => std::process::exit(1),
            }
       }
    })
}

make_static_var_and_getter!(get_id_size, ID_SIZE, usize);
make_static_var_and_getter!(get_bucket_size, BUCKET_SIZE, usize);
make_static_var_and_getter!(get_nr_shards, NR_SHARDS, usize);
make_static_var_and_getter!(get_shard_size, SHARD_SIZE, usize);

fn read_bucket(mut file: &File, addr: u64, len: u64) -> Vec<(u32, u8, u8)> {
    let id_size = get_id_size();
    let bk_size = get_bucket_size();
    file.seek(SeekFrom::Start(addr)).unwrap();
    let mut handle = file.take((bk_size * id_size) as u64);
    let mut buf=vec![0u8; bk_size*id_size];
    handle.read(&mut buf).unwrap();

    let vlen = len as usize;
    let mut vector = Vec::<(u32, u8, u8)>::with_capacity(vlen);
    for i in 0..vlen {
        let j = i*id_size;
        vector.push((LittleEndian::read_u32(&buf[j..j+4]), buf[j+4], buf[j+5]));
    }

    vector
}

// reading part
#[inline]
fn get_addr_and_len(ngram: &str, map: &fst::Map) -> Option<(u64, u64)> {
    match map.get(ngram) {
        Some(val) => {
            return Some(util::elegant_pair_inv(val))
        },
        None => return None,
    }
}

fn get_shard_ids(ngrams: &HashMap<String, f32>,
                 map: &fst::Map,
                 ifd: &File,
                 count: usize) -> Result<Vec<(u64, f32)>, Error>{

    let mut _ids = HashMap::new();
    let id_size = *get_id_size();
    let n = *get_shard_size() as f32;
    for (ngram, ntr) in ngrams {

        match get_addr_and_len(ngram, &map) {
            Some((addr, len)) => {
                for pqid_rem_tr in read_bucket(&ifd, addr*id_size as u64, len).iter() {

                    let pqid = pqid_rem_tr.0;
                    let reminder = pqid_rem_tr.1;
                    let qid = util::pqid2qid(pqid as u64, reminder, *get_nr_shards());


                    // println!("{:?} {:?} {:?}", ngram, qid, sc);
                    // TODO cosine similarity, normalize ngrams relevance at indexing time
                    // *sc += weight * ntr;
                    let tr = pqid_rem_tr.2;
                    let mut weight = (tr as f32)/100.0 ;
                    weight = util::max(0.0, ntr - (ntr - weight).abs() as f32);

                    let sc = _ids.entry(qid).or_insert(0.0);
                    *sc += weight * (n/len as f32).log(2.0);
                }
            },
            None => (),
        }
    }

    let mut v: Vec<(u64, f32)> = _ids.iter().map(|(id, sc)| (*id, *sc)).collect::<Vec<_>>();
    v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Less).reverse());
    v.truncate(count);
    Ok(v)
}

use std::cell::RefCell;
pub struct Qpick {
    path: String,
    config: config::Config,
    stopwords: HashSet<String>,
    terms_relevance: fst::Map,
    shards: Arc<Vec<Shard>>,
    thread_pool: RefCell<Pool>,
}

pub struct Shard {
    id: u32,
    map: fst::Map,
    shard: File,
}

pub struct QpickResults {
    pub items_iter: std::vec::IntoIter<(u64, f32)>,
}

impl QpickResults {
    pub fn new(items_iter: std::vec::IntoIter<(u64, f32)>) -> QpickResults {
        QpickResults { items_iter: items_iter }
    }

    pub fn next(&mut self) -> Option<(u64, f32)> {
        <std::vec::IntoIter<(u64, f32)> as std::iter::Iterator>::next(&mut self.items_iter)
    }
}

impl Qpick {
    fn new(path: String) -> Qpick {

        let c = config::Config::init();

        unsafe {
            // TODO set up globals, later should be available via self.config
            NR_SHARDS = Some(c.nr_shards);
            ID_SIZE = Some(c.id_size);
            BUCKET_SIZE = Some(c.bucket_size);
            SHARD_SIZE = Some(c.shard_size);
        }

        let stopwords = match stopwords::load(&c.stopwords_path) {
            Ok(stopwords) => stopwords,
            Err(_) => panic!("Failed to load stop-words!")
        };

        let terms_relevance = match Map::from_path(&c.terms_relevance_path) {
            Ok(terms_relevance) => terms_relevance,
            Err(_) => panic!("Failed to load terms rel. map: {}!", &c.terms_relevance_path)
        };

        let mut shards = vec![];
        for i in 0..c.nr_shards {
            let map_name = format!("{}/map.{}", path, i);
            let map = match Map::from_path(&map_name) {
                Ok(map) => map,
                Err(_) => panic!("Failed to load index map: {}!", &map_name)
            };

            let shard = OpenOptions::new().read(true).open(format!("{}/shard.{}", path, i)).unwrap();
            shards.push(Shard{id: i as u32, shard: shard, map: map});
        };

        let thread_pool = Pool::new(c.nr_shards as u32);

        Qpick {
            config: c,
            path: path,
            stopwords: stopwords,
            terms_relevance: terms_relevance,
            shards: Arc::new(shards),
            thread_pool: RefCell::new(thread_pool),
        }
    }

    pub fn from_path(path: String) -> Self {
        Qpick::new(path)
    }
    fn get_ids(&self, query: String, count: Option<usize>) -> Result<Vec<(u64, f32)>, Error> {

        if query == "" || count == Some(0) || count == None {
            return Ok(vec![])
        }

        let shard_count = match count {
           Some(1...50) => 100,
                 _ => count.unwrap(),
        };

        let mut _ids: Arc<Mutex<HashMap<u64, f32>>> = Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = mpsc::channel();

        let ref ngrams: HashMap<String, f32> = ngrams::parse(
            &query, 2, &self.stopwords, &self.terms_relevance, ngrams::ParseMode::Searching);

        let ref mut shards_ngrams: HashMap<usize, HashMap<String, f32>> = HashMap::new();

        for (ngram, sc) in ngrams {
            let shard_id = util::jump_consistent_hash_str(ngram, self.config.nr_shards as u32);

            let sh_ngrams = shards_ngrams.entry(shard_id as usize).or_insert(HashMap::new());
            sh_ngrams.insert(ngram.to_string(), *sc);
        }

        let nr_threads = shards_ngrams.len();

        self.thread_pool.borrow_mut().scoped(|scoped| {

            for sh_id_sh_ngram in shards_ngrams {

                let j = *sh_id_sh_ngram.0;
                let sh_ngrams = sh_id_sh_ngram.1.clone();
                let sender = sender.clone();
                let _ids = _ids.clone();
                let shards = self.shards.clone();

                scoped.execute(move || {

                    let sh_ids = match get_shard_ids(&sh_ngrams,
                                                     &shards[j].map,
                                                     &shards[j].shard,
                                                     shard_count) {
                        Ok(ids) => ids,
                        Err(_) => {
                            println!("Failed to retrive ids from shard: {}", &shards[j].id);
                            vec![]
                        }
                    };

                    // obtaining lock might fail! handle it!
                    let mut _ids = match _ids.lock() {
                        Ok(_ids) => _ids,
                        Err(poisoned) => poisoned.into_inner(),
                    };

                    for (qid, qsc) in sh_ids {
                        // qid is u64, qsc is f32
                        let sc = _ids.entry(qid).or_insert(0.0);
                        *sc += qsc
                    }
                    sender.send(()).unwrap();
                });
            }

        });

        for _ in 0..nr_threads {
            receiver.recv().unwrap();
        }

        // deref MutexGuard returned from _ids.lock().unwrap()
        let data = (*_ids.lock().unwrap()).clone();

        // turn for into vec and sort, that is expected from python extension
        // TODO avoid sorting per shard and turning into vector and sorting again,
        //      use a different data structure
        let mut vdata: Vec<(u64, f32)> = data.into_iter().map(|(id, sc)| (id, sc)).collect();
        vdata.sort_by(|a, b| {a.1.partial_cmp(&b.1).unwrap_or(Ordering::Less).reverse()});
        vdata.truncate(count.unwrap_or(100)); //TODO put into config
        Ok(vdata)
    }

    // TODO deprecated, to be removed
    pub fn search(&self, query: &str) -> String {
        let ids = match self.get_ids(query.to_string(), Some(100)) {
            Ok(ids) => serde_json::to_string(&ids).unwrap(),
            Err(err) => err.to_string(),
        };

        ids
    }

    pub fn get(&self, query: &str, count: u32) -> QpickResults {
        let ids = self.get_ids(query.to_string(), Some(count as usize)).unwrap();
        QpickResults::new(ids.into_iter())
    }

    pub fn merge(&self) -> Result<(), Error> {
        println!("Merging index maps from: {:?}", &self.path);
        merge::merge(&self.path, self.config.nr_shards as usize)
    }

    pub fn shard(file_path: String, nr_shards: usize, output_dir: String) -> Result<(), std::io::Error> {
        println!("Creating {:?} shards from {:?} to {:?}", nr_shards, file_path, output_dir);
        shard::shard(&file_path, nr_shards, &output_dir)
    }

    pub fn index(input_dir: String, shard_name: String,
                 first_shard: usize, last_shard: usize,
                 output_dir: String) -> Result<(), Error> {

        println!("Compiling {:?} shards from {:?} {:?} to {:?}",
                 last_shard-first_shard, input_dir, shard_name, output_dir);

        builder::index(&input_dir, &shard_name, first_shard, last_shard, &output_dir)
    }
}

#[allow(dead_code)]
fn main() {}
