use crossbeam_channel::Receiver;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use thiserror::Error;
use crate::{Transaction, TransactionStatus};
use crate::transaction::round;

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
    WrongClientId(u32, u32),
    #[error("Transaction ID {0} not unique")]
    DuplicatedTransaction(u32),
    #[error("Negative amount {0} in transaction {1}")]
    NegativeAmount(f32, u32),
    #[error("Not enough founds to withdraw {0} during transaction {1}, with available founds {2}")]
    InsufficientFounds(f32, u32, f32),
    #[error("Customer {0} is frozen and cannot perform withdraw transaction {1}")]
    CustomerFrozen(u32, u32),
    #[error("Transaction {0} could not complete")]
    NonExistingTransaction(u32),
    #[error("Cannot start a dispute on transaction {0} while being on status {:?}", 1)]
    InvalidStatusToStartDispute(u32, TransactionStatus),
    #[error("Cannot resolve a dispute on transaction {0} while being on status {:?}", 1)]
    InvalidStatusToResolve(u32, TransactionStatus),
    #[error("Cannot chargeback a dispute on transaction {0} while being on status {:?}", 1)]
    InvalidStatusToChargeback(u32, TransactionStatus),
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
            Transaction::Deposit { tx, client, .. }
            | Transaction::Withdrawal { tx, client, ..} if client == id && transaction_statuses.contains_key(&tx) => {
                errors.lock().unwrap().push(Box::new(ClientStatusError::DuplicatedTransaction(tx)));
            }
            Transaction::Deposit { tx, amount, client } if client == id && (amount > 0f32 || amount.abs() < f32::EPSILON) => {
                available += amount;
                transaction_statuses.insert(tx, (TransactionStatus::Deposited, amount));
            }
            Transaction::Deposit { tx, client, amount } if client == id => {
                errors.lock().unwrap().push(Box::new(ClientStatusError::NegativeAmount(amount, tx)));
                transaction_statuses.insert(tx, (TransactionStatus::FailedDeposit, 0f32));
            }
            Transaction::Withdrawal { tx, amount, client }
                if client == id && !locked && (amount < available || (amount - available).abs() < f32::EPSILON) && (amount > 0f32 || amount.abs() < f32::EPSILON) => {
                available -= amount;
                transaction_statuses.insert(tx, (TransactionStatus::Withdrew, -amount));
            }
            Transaction::Withdrawal { tx, client, amount } if client == id && !locked && amount < 0f32 => {
                errors.lock().unwrap().push(Box::new(ClientStatusError::NegativeAmount(amount, tx)));
                transaction_statuses.insert(tx, (TransactionStatus::FailedWithdrawal, 0f32));
            }
            Transaction::Withdrawal { tx, client, amount } if client == id && !locked => {
                errors.lock().unwrap().push(Box::new(ClientStatusError::InsufficientFounds(amount, tx, available)));
                transaction_statuses.insert(tx, (TransactionStatus::FailedWithdrawal, 0f32));
            }
            Transaction::Withdrawal { tx, client, .. } if client == id => {
                errors.lock().unwrap().push(Box::new(ClientStatusError::CustomerFrozen(client, tx)));
                transaction_statuses.insert(tx, (TransactionStatus::FailedWithdrawal, 0f32));
            }
            Transaction::Dispute { tx, client } if client == id => {
                match transaction_statuses.get(&tx).cloned() {
                    Some((TransactionStatus::Deposited, amount)) | Some((TransactionStatus::Resolved, amount)) => {
                        held += amount;
                        available -= amount;
                        transaction_statuses.insert(tx, (TransactionStatus::OnDispute, amount));
                    }
                    Some((status, _)) => {
                        errors.lock().unwrap().push(Box::new(ClientStatusError::InvalidStatusToStartDispute(tx, status)));
                    }
                    None => {
                        errors.lock().unwrap().push(Box::new(ClientStatusError::NonExistingTransaction(tx)));
                    }
                }
            }
            Transaction::Resolve { tx, client } if client == id => {
                match transaction_statuses.get(&tx).cloned() {
                    Some((TransactionStatus::OnDispute, amount)) => {
                        held -= amount;
                        available += amount;
                        transaction_statuses.insert(tx, (TransactionStatus::Resolved, amount));
                    }
                    Some((status, _)) => {
                        errors.lock().unwrap().push(Box::new(ClientStatusError::InvalidStatusToResolve(tx, status)));
                    }
                    None => {
                        errors.lock().unwrap().push(Box::new(ClientStatusError::NonExistingTransaction(tx)));
                    }
                }
            }
            Transaction::Chargeback { tx, client } if client == id => {
                match transaction_statuses.get(&tx).cloned() {
                    Some((TransactionStatus::OnDispute, amount)) => {
                        held -= amount;
                        locked = true;
                        transaction_statuses.insert(tx, (TransactionStatus::Chargeback, amount));
                    }
                    Some((status, _)) => {
                        errors.lock().unwrap().push(Box::new(ClientStatusError::InvalidStatusToChargeback(tx, status)));
                    }
                    None => {
                        errors.lock().unwrap().push(Box::new(ClientStatusError::NonExistingTransaction(tx)));
                    }
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
        ClientStatus { id, available: round(available), held: round(held), locked, total: round(held + available) }
    );
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::thread::JoinHandle;
    use crossbeam_channel::unbounded;
    use crate::client_status::{build, ClientStatusError};
    use crate::{ClientStatus, Transaction, TransactionStatus};

    #[test]
    fn four_point_precision() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1.123123f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2.111111f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.222222f32 },
        ];
        test_successful_transaction(1, transactions, ClientStatus {
            id: 1,
            available: 2.012f32,
            held: 0f32,
            total: 2.012f32,
            locked: false,
        });
    }

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
        test_transaction_with_errors(2, transactions, ClientStatus {
            id: 2,
            available: 2f32,
            held: 0f32,
            total: 2f32,
            locked: false,
        }, vec![ClientStatusError::InsufficientFounds(3f32, 5, 2f32)]);
    }

    #[test]
    fn test_negative_deposit() {
        let transactions = vec![
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Deposit { client: 2, tx: 5, amount: -3f32 },
        ];
        test_transaction_with_errors(2, transactions, ClientStatus {
            id: 2,
            available: 2f32,
            held: 0f32,
            total: 2f32,
            locked: false,
        }, vec![ClientStatusError::NegativeAmount(-3f32, 5)]);
    }

    #[test]
    fn test_negative_withdrawal() {
        let transactions = vec![
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Withdrawal { client: 2, tx: 5, amount: -3f32 },
        ];
        test_transaction_with_errors(2, transactions, ClientStatus {
            id: 2,
            available: 2f32,
            held: 0f32,
            total: 2f32,
            locked: false,
        }, vec![ClientStatusError::NegativeAmount(-3f32, 5)]);
    }

    #[test]
    fn test_it_only_process_relevant_client() {
        let transactions = vec![
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Withdrawal { client: 2, tx: 5, amount: 3f32 },
        ];
        test_transaction_with_errors(1, transactions, ClientStatus {
            id: 1,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false
        }, vec![ClientStatusError::WrongClientId(1, 2), ClientStatusError::WrongClientId(1, 2)]);
    }

    #[test]
    fn test_transactions_are_unique() {
        let transactions = vec![
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Withdrawal { client: 2, tx: 5, amount: 1f32 },
            Transaction::Deposit { client: 2, tx: 2, amount: 2f32 },
            Transaction::Withdrawal { client: 2, tx: 5, amount: 3f32 },
        ];
        test_transaction_with_errors(2, transactions, ClientStatus {
            id: 2,
            available: 1.0,
            held: 0.0,
            total: 1.0,
            locked: false
        }, vec![ClientStatusError::DuplicatedTransaction(2), ClientStatusError::DuplicatedTransaction(5)]);
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
        test_transaction_with_errors(1, transactions, ClientStatus {
            id: 1,
            available: 1.5f32,
            held: 0f32,
            total: 1.5f32,
            locked: false,
        }, vec![ClientStatusError::InvalidStatusToChargeback(1, TransactionStatus::Deposited), ClientStatusError::InvalidStatusToResolve(1, TransactionStatus::Deposited)]);
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
    fn test_dispute_resolve_makes_funds_available_with_duplicate_resolves() {
        let transactions = vec![
            Transaction::Deposit { client: 1, tx: 1, amount: 1f32 },
            Transaction::Deposit { client: 1, tx: 3, amount: 2f32 },
            Transaction::Withdrawal { client: 1, tx: 4, amount: 1.5f32 },
            Transaction::Dispute { client: 1, tx: 1, },
            Transaction::Resolve { client: 1, tx: 1, },
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
            Transaction::Withdrawal { client: 1, tx: 5, amount: 0.5f32 },
            Transaction::Deposit { client: 1, tx: 6, amount: 2f32 },
        ];
        test_transaction_with_errors(1, transactions, ClientStatus {
            id: 1,
            available: 2.5f32,
            held: 0f32,
            total: 2.5f32,
            locked: true,
        }, vec![ClientStatusError::CustomerFrozen(1, 5)]);
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

    fn test_transaction_with_errors(
        client_id: u32,
        transactions: Vec<Transaction>,
        client_status: ClientStatus,
        expected_errors: Vec<ClientStatusError>,
    ) {
        let result = Arc::new(Mutex::new(vec![]));
        let errors: Arc<Mutex<Vec<Box<dyn Error + Send>>>> = Arc::new(Mutex::new(vec![]));
        run_build(client_id, transactions, result.clone(), errors.clone()).join().unwrap();
        let errors = Arc::try_unwrap(errors).unwrap().into_inner().unwrap();
        let result = Arc::try_unwrap(result).unwrap().into_inner().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], client_status);
        assert_eq!(errors.len(), expected_errors.len());
        for (e1, e2) in errors.iter().zip(expected_errors.iter()) {
            assert_eq!(e1.to_string(), e2.to_string());
        }
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