use std::collections::HashMap;
use std::error::Error;
use std::io::Read;
use std::sync::{Arc, Mutex};
use crossbeam_channel::unbounded;
use csv::Trim;
use threadpool::ThreadPool;
use client_status::ClientStatus;
use transaction::{RawTransaction, Transaction, TransactionStatus};

mod transaction;
mod client_status;

pub fn execute_transactions<R: Read>(reader: R, threads: usize) -> (Vec<ClientStatus>, Vec<Box<dyn Error + Send>>) {
    let pool = ThreadPool::new(threads);
    let result = Arc::new(Mutex::new(vec![]));
    let errors: Arc<Mutex<Vec<Box<dyn Error + Send>>>> = Arc::new(Mutex::new(vec![]));

    process_transactions(reader, &pool, &result, &errors);
    pool.join();

    (
        Arc::try_unwrap(result).unwrap().into_inner().unwrap(),
        Arc::try_unwrap(errors).unwrap().into_inner().unwrap()
    )
}

fn process_transactions<R: Read>(
    reader: R,
    pool: &ThreadPool,
    result: &Arc<Mutex<Vec<ClientStatus>>>,
    errors: &Arc<Mutex<Vec<Box<dyn Error + Send>>>>
) {
    let mut beams = HashMap::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .from_reader(reader);
    for raw_transaction in reader.deserialize::<RawTransaction>() {
        let raw_transaction = match raw_transaction {
            Ok(rt) => rt,
            Err(e) => {
                let mut errors = errors.lock().unwrap();
                errors.push(Box::new(e));
                continue;
            }
        };
        let transaction: Transaction = match raw_transaction.try_into() {
            Ok(transaction) => transaction,
            Err(e) => {
                let mut errors = errors.lock().unwrap();
                errors.push(Box::new(e));
                continue;
            }
        };
        let client = transaction.get_client();
        let sender = match beams.get(&client) {
            Some(sender) => sender,
            None => {
                let (sender, receiver) = unbounded();
                let pool_result = result.clone();
                let pool_errors = errors.clone();
                pool.execute(move || client_status::build(client, receiver, pool_result, pool_errors));
                beams.insert(client, sender);
                beams.get(&client).unwrap()
            }
        };
        if let Err(e) = sender.send(transaction) {
            let mut errors = errors.lock().unwrap();
            errors.push(Box::new(e));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use crate::{ClientStatus, execute_transactions};
    use crate::client_status::ClientStatusError;
    use crate::transaction::TransactionParseError;

    #[test]
    fn test_process_transactions() {
        test_result(
            "type, client,tx,amount\ndeposit, 1,1,1.0\ndeposit,2,2,2.0\ndeposit,1,3,2.0\nwithdrawal,1,4,1.5\nwithdrawal,2,5,3.0",
            vec![
                ClientStatus { id: 1, available: 1.5, held: 0.0, total: 1.5, locked: false },
                ClientStatus { id: 2, available: 2.0, held: 0.0, total: 2.0, locked: false }
            ],
            vec![Box::new(ClientStatusError::InsufficientFounds(3f32, 5, 2f32))]
        );
    }

    #[test]
    fn test_process_transaction_with_wrong_transaction_types() {
        test_result(
            "type, client,tx,amount\ndeposit, 1,1,1.0\ndeposit,2,2,2.0\ndeposit,1,3,2.0\nwithdrawal,1,4,1.5\nwithdrawal42,2,5,3.0",
            vec![
                ClientStatus { id: 1, available: 1.5, held: 0.0, total: 1.5, locked: false },
                ClientStatus { id: 2, available: 2.0, held: 0.0, total: 2.0, locked: false }
            ],
            vec![Box::new(TransactionParseError::InvalidTransactionType("withdrawal42".to_string()))]
        );
    }

    fn test_result(transactions: &str, expected_results: Vec<ClientStatus>, expected_errors: Vec<Box<dyn Error + Send>>) {
        let (mut result, errors) = execute_transactions(transactions.as_bytes(), 1);
        result.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(expected_results, result);
        assert_eq!(expected_errors.len(), errors.len());
        for (e1, e2) in expected_errors.iter().zip(&errors) {
            assert_eq!(e1.to_string(), e2.to_string())
        }
    }
}