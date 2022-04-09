use crossbeam_channel::Receiver;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use thiserror::Error;
use crate::{Transaction, TransactionStatus};

#[derive(Clone, Debug, PartialEq)]
pub struct ClientStatus {
    pub(crate) id: u32,
    pub(crate) available: f32,
    pub(crate) held: f32,
    pub(crate) total: f32,
    pub(crate) locked: bool,
}

impl ClientStatus {
    pub fn to_record(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.available.to_string(),
            self.held.to_string(),
            self.total.to_string(),
            self.locked.to_string(),
        ]
    }
}

#[derive(Debug, Error)]
pub enum ClientStatusError {
    #[error("Builder expected transactions for client {0}, but got one for client {1}")]
    WrongClientId(u32, u32)
}

pub fn build(
    id: u32,
    receiver: Receiver<Transaction>,
    result: Arc<Mutex<Vec<ClientStatus>>>,
    errors: Arc<Mutex<Vec<Box<dyn std::error::Error + Send>>>>,
) {
    let mut available = 0f32;
    let mut held = 0f32;
    let mut locked = false;
    let mut transaction_statuses = HashMap::new();

    for t in receiver {
        match t {
            Transaction::Deposit { tx, amount, client } if client == id => {
                available += amount;
                transaction_statuses.insert(tx, (TransactionStatus::Complete, amount));
            }
            Transaction::Withdrawal { tx, amount, client } if client == id && !locked && amount <= available => {
                available -= amount;
                transaction_statuses.insert(tx, (TransactionStatus::Complete, -amount));
            }
            Transaction::Withdrawal { tx, client, .. } if client == id => {
                transaction_statuses.insert(tx, (TransactionStatus::Failed, 0f32));
            }
            Transaction::Dispute { tx, client } if client == id => {
                if let Some((TransactionStatus::Complete, amount)) | Some((TransactionStatus::Failed, amount)) = transaction_statuses.get(&tx).cloned() {
                    held += amount;
                    available -= amount;
                    transaction_statuses.insert(tx, (TransactionStatus::OnDispute, amount));
                }
            }
            Transaction::Resolve { tx, client } if client == id => {
                if let Some((TransactionStatus::OnDispute, amount)) = transaction_statuses.get(&tx).cloned() {
                    held -= amount;
                    available += amount;
                    transaction_statuses.insert(tx, (TransactionStatus::Resolved, amount));
                }
            }
            Transaction::Chargeback { tx, client } if client == id => {
                if let Some((TransactionStatus::OnDispute, amount)) = transaction_statuses.get(&tx).cloned() {
                    held -= amount;
                    locked = true;
                    transaction_statuses.insert(tx, (TransactionStatus::Chargeback, amount));
                }
            }
            Transaction::Deposit { client, .. } | Transaction::Withdrawal { client, ..} |
                Transaction::Dispute { client, .. } | Transaction::Resolve { client, .. } |
                Transaction::Chargeback { client, .. } => {
                errors.lock().unwrap().push(Box::new(ClientStatusError::WrongClientId(id, client)));
            },
        }
    }

    let mut result = result.lock().unwrap();
    result.push(
        ClientStatus { id, available, held, locked, total: held + available }
    );
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::thread::JoinHandle;
    use crossbeam_channel::unbounded;
    use crate::client_status::build;
    use crate::{ClientStatus, Transaction};

    #[test]
    fn test_deposit_and_withdrawal_without_failed_withdrawal() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 1.5f32,
            held: 0f32,
            total: 1.5f32,
            locked: false,
        });
    }

    #[test]
    fn test_deposit_and_withdrawal_with_failed_withdrawal() {
        let transactions = vec![
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Withdrawal { client: 2, tx: 5, amount: 3f32 },
        ];
        test_successful_transaction(2, transactions, ClientStatus {
            id: 2,
            available: 2f32,
            held: 0f32,
            total: 2f32,
            locked: false,
        });
    }

    #[test]
    #[should_panic(expected = "WrongClientId(1, 2)")]
    fn test_it_only_process_relevant_client() {
        let transactions = vec![
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Withdrawal { client: 2, tx: 5, amount: 3f32 },
        ];
        let result = Arc::new(Mutex::new(vec![]));
        let errors: Arc<Mutex<Vec<Box<dyn Error + Send>>>> = Arc::new(Mutex::new(vec![]));
        run_build(1, transactions, result.clone(), errors.clone()).join().unwrap();
        let errors = Arc::try_unwrap(errors).unwrap().into_inner().unwrap();
        assert_eq!(errors.len(), 2);
        let r: Result<(), Box<dyn Error>> = Err(errors.into_iter().next().unwrap());
        r.unwrap();
    }

    #[test]
    fn test_dispute_puts_funds_on_hold() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
            Transaction::Dispute { client: 1, tx: 1, },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 0.5f32,
            held: 1f32,
            total: 1.5f32,
            locked: false,
        });
    }

    #[test]
    fn test_dispute_resolve_and_chargeback_on_non_dispute_tx_do_nothing() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
            Transaction::Chargeback { client: 1, tx: 1, },
            Transaction::Resolve { client: 1, tx: 1, },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 1.5f32,
            held: 0f32,
            total: 1.5f32,
            locked: false,
        });
    }

    #[test]
    fn test_dispute_resolve_makes_funds_available() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
            Transaction::Dispute { client: 1, tx: 1, },
            Transaction::Resolve { client: 1, tx: 1, },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 1.5f32,
            held: 0f32,
            total: 1.5f32,
            locked: false,
        });
    }

    #[test]
    fn test_dispute_chargeback_freezes_and_removes_funds() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
            Transaction::Dispute { client: 1, tx: 1, },
            Transaction::Chargeback { client: 1, tx: 1, },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 0.5f32,
            held: 0f32,
            total: 0.5f32,
            locked: true,
        });
    }

    #[test]
    fn test_frozen_account_can_deposit_and_not_withdraw() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
            Transaction::Dispute { client: 1, tx: 1, },
            Transaction::Chargeback { client: 1, tx: 1, },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 0.5f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 2.5f32,
            held: 0f32,
            total: 2.5f32,
            locked: true,
        });
    }

    fn test_successful_transaction(
        client_id: u32,
        transactions: Vec<Transaction>,
        client_status: ClientStatus,
    ) {
        let result = Arc::new(Mutex::new(vec![]));
        let errors: Arc<Mutex<Vec<Box<dyn Error + Send>>>> = Arc::new(Mutex::new(vec![]));
        run_build(client_id, transactions, result.clone(), errors.clone()).join().unwrap();
        let errors = Arc::try_unwrap(errors).unwrap().into_inner().unwrap();
        let result = Arc::try_unwrap(result).unwrap().into_inner().unwrap();
        assert!(errors.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], client_status);
    }

    fn run_build(
        client_id: u32,
        transactions: Vec<Transaction>,
        result: Arc<Mutex<Vec<ClientStatus>>>,
        errors: Arc<Mutex<Vec<Box<dyn Error + Send>>>>
    ) -> JoinHandle<()> {
        let (sender, receiver) = unbounded();
        let j = thread::spawn(move || build(client_id, receiver, result, errors));
        for t in transactions {
            sender.send(t).unwrap();
        }
        j
    }
}