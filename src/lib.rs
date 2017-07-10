extern crate fst;
extern crate byteorder;
extern crate serde_json;

use fst::Regex;
use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::str;

use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::collections::HashSet;

use byteorder::{ByteOrder, LittleEndian};
use fst::{IntoStreamer, Streamer, Map};
use std::io::SeekFrom;

use fst::Error;

pub mod util;
pub mod config;
pub mod ngrams;
pub mod merge;
pub mod builder;
pub mod stopwords;

use std::collections::BinaryHeap;

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct QpickItem {
    qid: u64,
    sc: u32,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait to turn the queue into a min-heap (by default it's a max-heap).
impl Ord for QpickItem {
    fn cmp(&self, other: &QpickItem) -> Ordering {
        other.sc.cmp(&self.sc) // flipped ordering here!
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for QpickItem {
    fn partial_cmp(&self, other: &QpickItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


fn read_bucket(mut file: &File, addr: u64, len: u64) -> Vec<(u32, u8)> {
    let id_size = get_id_size();
    let bk_size = get_bucket_size();
    file.seek(SeekFrom::Start(addr)).unwrap();
    let mut handle = file.take((bk_size * id_size) as u64);
    let mut buf=vec![0u8; bk_size*id_size];
    handle.read(&mut buf).unwrap();

    let vlen = len as usize;
    let mut vector = Vec::<(u32, u8)>::with_capacity(vlen);
    for i in 0..vlen {
        let j = i*id_size;
        vector.push((LittleEndian::read_u32(&buf[j..j+4]), buf[j+4]));
    }

    vector
}

fn get_shard_ids(vals: &Vec<(u64, f32)>,
                 shard: &Shard,
                 count: Option<usize>) -> Result<Vec<(u64, f32)>, Error>{

    let mut _ids = HashMap::new();
    let id_size = *get_id_size();
    let n = *get_shard_size() as f32;

    for val_ntr in vals.iter() {

        let (val, ntr) = *val_ntr;

        let (addr, len) = util::elegant_pair_inv(val);

        for id_tr in read_bucket(&shard.shard, addr*id_size as u64, len).iter() {
            let qid = util::pqid2qid(id_tr.0 as u64, shard.id as u64, *get_nr_shards());
            let sc = _ids.entry(qid).or_insert(0.0);
            // TODO cosine similarity, normalize ngrams relevance at indexing time
            // *sc += weight * ntr;
            let mut weight = (id_tr.1 as f32)/100.0 ;
            weight = util::max(0.0, ntr - (ntr - weight).abs() as f32);
            *sc += weight * (n/len as f32).log(2.0);
        }
    }

    let mut v: Vec<(u64, f32)> = _ids.iter().map(|(id, sc)| (*id, *sc)).collect::<Vec<_>>();
    v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Less).reverse());
    v.truncate(count.unwrap_or(100)); //TODO put into config
    Ok(v)
}

pub struct Qpick {
    path: String,
    config: config::Config,
    stopwords: HashSet<String>,
    terms_relevance: fst::Map,
    shards: Vec<Shard>,
    map: fst::Map,
}

pub struct Shard {
    id: u32,
    shard: File,
}

pub struct QpickResults {
    items_iter: std::vec::IntoIter<(u64, f32)>,
}

impl QpickResults {
    pub fn new(mut items_iter: std::vec::IntoIter<(u64, f32)>) -> QpickResults {
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
        for i in c.first_shard..c.last_shard {
            let shard = OpenOptions::new().read(true).open(format!("{}/shard.{}", path, i)).unwrap();
            shards.push(Shard{id: i, shard: shard});
        };

        let map_name = format!("{}/union_map.{}.fst", path, c.last_shard);
        let map = match Map::from_path(&map_name) {
            Ok(map) => map,
            Err(_) => panic!("Failed to load index map: {}!", &map_name)
        };

        Qpick {
            config: c,
            path: path,
            stopwords: stopwords,
            terms_relevance: terms_relevance,
            shards: shards,
            map: map,
        }
    }

    pub fn from_path(path: String) -> Self {
        Qpick::new(path)
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    fn get_ids(&self, query: String) -> Result<Vec<Vec<(u64, f32)>>, Error> {

        let mut _ids: Vec<Vec<(u64, f32)>> = vec![vec![]; self.config.nr_shards];

        let ref ngrams: HashMap<String, f32> = ngrams::parse(
            &query, 2, &self.stopwords, &self.terms_relevance, ngrams::ParseMode::Searching);

        // each vector is collection of tuples of (addr, len) pair and ngram's term relevance
        let ref mut pids2qids: Vec<Vec<(u64, f32)>> = vec![vec![]; self.config.nr_shards];

        // get ngrams with paired (addr, len) values from the map, group them per shard
        for (ngram, ntr) in ngrams {
            let re_str = format!("{}:\\d+", ngram);
            let re = try!(Regex::new(&re_str));
            let mut stream = self.map.search(&re).into_stream();

            while let Some((k, v)) = stream.next() {
                let key = str::from_utf8(&k).unwrap();
                let (_, pid) = util::key2ngram(key.to_string());
                pids2qids[pid as usize].push((v, *ntr));
            }
        }

        for j in 0..self.config.nr_shards {

            if pids2qids[j].len() == 0 {
                continue;
            }

            let sh_ids = match get_shard_ids(&pids2qids[j], &self.shards[j], None) {
                Ok(ids) => ids,
                Err(_) => {
                    println!("Failed to retrive ids from shard: {}", j);
                    vec![]
                }
            };

            _ids[j as usize] = sh_ids;
        }

        Ok(_ids)
    }

    pub fn get(&self, query: &str, count: u32) -> QpickResults {

        if query == "" || count == 0 {
            return QpickResults::new((vec![]).into_iter())
        }

        let mut _ids: BinaryHeap<QpickItem> = BinaryHeap::new();
        let ref ngrams: HashMap<String, f32> = ngrams::parse(
            &query, 2, &self.stopwords, &self.terms_relevance, ngrams::ParseMode::Searching);

        // each vector is collection of tuples of (addr, len) pair and ngram's term relevance
        let ref mut pids2qids: Vec<Vec<(u64, f32)>> = vec![vec![]; self.config.nr_shards];

        // get ngrams with paired (addr, len) values from the map, group them per shard
        for (ngram, ntr) in ngrams {
            let re_str = format!("{}:\\d+", ngram);
            let re = Regex::new(&re_str).unwrap();
            let mut stream = self.map.search(&re).into_stream();

            while let Some((k, v)) = stream.next() {
                let key = str::from_utf8(&k).unwrap();
                let (_, pid) = util::key2ngram(key.to_string());
                pids2qids[pid as usize].push((v, *ntr));
            }
        }

        let shard_count = match count {
            1...10 => 10 * count,
           10...20 =>  5 * count,
           20...30 =>  3 * count,
           30...50 =>  2 * count,
                 _ => count,
        };

        for j in 0..self.config.nr_shards {

            if pids2qids[j].len() == 0 {
                continue;
            }

            let sh_ids = match get_shard_ids(&pids2qids[j], &self.shards[j], Some(shard_count as usize)) {
                Ok(ids) => ids,
                Err(_) => {
                    println!("Failed to retrive ids from shard: {}", j);
                    vec![]
                }
            };

            for sh_id in sh_ids {
                // rounding to 3 decimal places and casting to u32, because Eq doesn't exist for f32
                let qpi = QpickItem{qid: sh_id.0, sc: (sh_id.1 * 1000.0).round() as u32};

                if _ids.len() >= count as usize {
                    let mut min_id = _ids.peek_mut().unwrap();
                    if qpi < *min_id {
                        *min_id = qpi
                    }
                } else {
                    _ids.push(qpi);
                }
            }
        }

        let mut ids = _ids.iter().map(|qpi| (qpi.qid, qpi.sc as f32/1000.0)).collect::<Vec<(u64, f32)>>();
        QpickResults::new(ids.into_iter())
    }

    pub fn search(&self, query: &str) -> String {
        let ids = match self.get_ids(query.to_string()) {
            Ok(ids) => serde_json::to_string(&ids).unwrap(),
            Err(err) => err.to_string(),
        };

        ids
    }

    pub fn merge(path: String, nr_shards: usize) -> Result<(), Error> {
        println!("Merging {:?} index maps from: {:?}", nr_shards, path);
        merge::merge(&path, nr_shards)
    }

}

#[allow(dead_code)]
fn main() {

    let c = config::Config::init();
    unsafe {
        NR_SHARDS = Some(c.nr_shards);
        ID_SIZE = Some(c.id_size);
        BUCKET_SIZE = Some(c.bucket_size);
        SHARD_SIZE = Some(c.shard_size);
    }

    let ref stopwords = match stopwords::load(&c.stopwords_path) {
        Ok(stopwords) => stopwords,
        Err(_) => panic!("Failed to load stop-words!")
    };

    let tr_map = match Map::from_path(&c.terms_relevance_path) {
        Ok(tr_map) => tr_map,
        Err(_) => panic!("Failed to load terms rel. map!")
    };
    let arc_tr_map = Arc::new(tr_map);

    let (sender, receiver) = mpsc::channel();

    for i in c.first_shard..c.last_shard {
        let sender = sender.clone();
        let tr_map = arc_tr_map.clone();
        let stopwords = stopwords.clone();

        let input_file_name = format!("{}/{}.{}.txt", c.dir_path, c.file_name, i);

        thread::spawn(move || {
            builder::build_shard(i, &input_file_name, &tr_map, &stopwords,
                                 *get_id_size(), *get_bucket_size(), *get_nr_shards()).unwrap();
            sender.send(()).unwrap();
        });
    }

    for _ in c.first_shard..c.last_shard {
        receiver.recv().unwrap();
    }

    println!("Compiled {} shards.", c.last_shard - c.first_shard);
}
