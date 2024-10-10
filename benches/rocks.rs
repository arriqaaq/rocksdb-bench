use std::path::Path;

use rocksdb::{DB, ColumnFamilyDescriptor, Options, WriteBatch};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;

const NUM_TABLES: usize = 10;
const NUM_KEYS_PER_TABLE: usize = 10000;

fn generate_key(table: usize, x: usize) -> String {
    format!("/table/{:02}/{:05}", table, x)
}

fn generate_value() -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(100)
        .map(char::from)
        .collect()
}

fn write_single_cf(db: &DB) {
    let mut batch = WriteBatch::default();
    for table in 0..NUM_TABLES {
        for x in 0..NUM_KEYS_PER_TABLE {
            let key = generate_key(table, x);
            let value = generate_value();
            batch.put(key.as_bytes(), value.as_bytes());
        }
    }
    db.write(batch).unwrap();
}

fn write_multiple_cf(db: &DB) {
    for table in 0..NUM_TABLES {
        let cf_name = format!("table_{}", table);
        let cf = db.cf_handle(&cf_name).unwrap();
        let mut batch = WriteBatch::default();
        for x in 0..NUM_KEYS_PER_TABLE {
            let key = generate_key(table, x);
            let value = generate_value();
            batch.put_cf(cf, key.as_bytes(), value.as_bytes());
        }
        db.write(batch).unwrap();
    }
}

fn write_multiple_dbs(dbs: &[DB]) {
    for (table, db) in dbs.iter().enumerate() {
        let mut batch = WriteBatch::default();
        for x in 0..NUM_KEYS_PER_TABLE {
            let key = generate_key(table, x);
            let value = generate_value();
            batch.put(key.as_bytes(), value.as_bytes());
        }
        db.write(batch).unwrap();
    }
}

fn range_query_single_cf(db: &DB, table: usize) {
    let start_key = generate_key(table, 0);
    let end_key = generate_key(table, NUM_KEYS_PER_TABLE);
    let iter = db.iterator(rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));
    
    let count = black_box({
        iter.take_while(|result| match result {
            Ok((k, _)) => k.as_ref() < end_key.as_bytes(),
            Err(_) => false,
        })
        .count()
    });
    
    assert_eq!(count, NUM_KEYS_PER_TABLE, "Count of items does not match NUM_KEYS_PER_TABLE");
}

fn range_query_multiple_cf(db: &DB, table: usize) {
    let cf_name = format!("table_{}", table);
    let cf = db.cf_handle(&cf_name).unwrap();
    let start_key = generate_key(table, 0);
    let end_key = generate_key(table, NUM_KEYS_PER_TABLE);
    let iter = db.iterator_cf(cf, rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));

    let count = black_box({
        iter.take_while(|result| match result {
            Ok((k, _)) => k.as_ref() < end_key.as_bytes(),
            Err(_) => false,
        })
        .count()
    });
    
    assert_eq!(count, NUM_KEYS_PER_TABLE, "Count of items does not match NUM_KEYS_PER_TABLE");
}

fn range_query_multiple_dbs(dbs: &[DB], table: usize) {
    assert!(table < NUM_TABLES, "Table index out of bounds");

    let db = &dbs[table];
    let start_key = generate_key(table, 0);
    let end_key = generate_key(table, NUM_KEYS_PER_TABLE);
    let iter = db.iterator(rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));

    let count = black_box({
        iter.take_while(|result| match result {
            Ok((k, _)) => k.as_ref() < end_key.as_bytes(),
            Err(_) => false,
        }).count()
    });

    assert_eq!(
        count,
        NUM_KEYS_PER_TABLE,
        "Count of items does not match NUM_KEYS_PER_TABLE"
    );
}

fn get_single_cf(db: &DB) {
    let mut rng = rand::thread_rng();
    for _ in 0..1000 {
        let table = rng.gen_range(0..NUM_TABLES);
        let x = rng.gen_range(0..NUM_KEYS_PER_TABLE);
        let key = generate_key(table, x);
        black_box(db.get(key.as_bytes()).unwrap());
    }
}

fn get_multiple_cf(db: &DB) {
    let mut rng = rand::thread_rng();
    for _ in 0..1000 {
        let table = rng.gen_range(0..NUM_TABLES);
        let x = rng.gen_range(0..NUM_KEYS_PER_TABLE);
        let cf_name = format!("table_{}", table);
        let cf = db.cf_handle(&cf_name).unwrap();
        let key = generate_key(table, x);
        black_box(db.get_cf(cf, key.as_bytes()).unwrap());
    }
}

fn get_multiple_dbs(dbs: &[DB]) {
    let mut rng = rand::thread_rng();
    for _ in 0..1000 {
        let table = rng.gen_range(0..NUM_TABLES);
        let x = rng.gen_range(0..NUM_KEYS_PER_TABLE);
        let db = &dbs[table];
        let key = generate_key(table, x);
        black_box(db.get(key.as_bytes()).unwrap());
    }
}

fn open_multiple_dbs(path: &Path) -> Vec<DB> {
    let mut dbs = Vec::with_capacity(NUM_TABLES);
    for table in 0..NUM_TABLES {
        let db_path = path.join(format!("db_{}", table));
        let db = DB::open_default(db_path).unwrap();
        dbs.push(db);
    }
    dbs
}


fn benchmark(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    // Single CF setup
    let db_single = DB::open_default(path.join("single_cf")).unwrap();

    // Multiple CF setup
    let mut cfs = vec![];
    for i in 0..NUM_TABLES {
        cfs.push(ColumnFamilyDescriptor::new(format!("table_{}", i), Options::default()));
    }

    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    let db_multiple = DB::open_cf_descriptors(&db_options, path.join("multiple_cf"), cfs).unwrap();

    // Multiple DBs setup
    let dbs_multiple = open_multiple_dbs(path);

    c.bench_function("write_single_cf", |b| b.iter(|| write_single_cf(&db_single)));
    c.bench_function("write_multiple_cf", |b| b.iter(|| write_multiple_cf(&db_multiple)));
    c.bench_function("write_multiple_dbs", |b| {
        b.iter(|| write_multiple_dbs(&dbs_multiple))
    });

    c.bench_function("range_query_single_cf", |b| b.iter(|| range_query_single_cf(&db_single, 5)));
    c.bench_function("range_query_multiple_cf", |b| b.iter(|| range_query_multiple_cf(&db_multiple, 5)));
    c.bench_function("range_query_multiple_dbs", |b| b.iter(|| range_query_multiple_dbs(&dbs_multiple, 5)));

    c.bench_function("get_single_cf", |b| b.iter(|| get_single_cf(&db_single)));
    c.bench_function("get_multiple_cf", |b| b.iter(|| get_multiple_cf(&db_multiple)));
    c.bench_function("get_multiple_dbs", |b| {
        b.iter(|| get_multiple_dbs(&dbs_multiple))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);