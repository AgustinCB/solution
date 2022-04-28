use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Deserialize)]
pub struct RawTransaction {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    transaction_type: String,
    client: u32,
    tx: u32,
    amount: f32,
}

#[derive(Debug, PartialEq)]
pub enum Transaction {
    Deposit {
        client: u32,
        tx: u32,
        amount: f32,
    },
    Withdrawal {
        client: u32,
        tx: u32,
        amount: f32,
    },
    Dispute {
        client: u32,
        tx: u32,
    },
    Resolve {
        client: u32,
        tx: u32,
    },
    Chargeback {
        client: u32,
        tx: u32
    },
}

impl Transaction {
    pub fn get_client(&self) -> u32 {
        match &self {
            Transaction::Deposit { client, .. } | Transaction::Withdrawal { client, ..} |
            Transaction::Dispute { client, .. } | Transaction::Resolve { client, .. } |
            Transaction::Chargeback { client, .. } => *client,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum TransactionParseError {
    #[error("Transaction type {0} is invalid")]
    InvalidTransactionType(String),
}

const PRECISION: f32 = 10000f32;

pub fn round(n: f32) -> f32 {
    (n * PRECISION).round() / (PRECISION)
}

impl TryInto<Transaction> for RawTransaction {
    type Error = TransactionParseError;

    fn try_into(self) -> Result<Transaction, Self::Error> {
        match self.transaction_type.as_str() {
            "deposit" => Ok(Transaction::Deposit {
                client: self.client,
                tx: self.tx,
                amount: round(self.amount),
            }),
            "withdrawal" => Ok(Transaction::Withdrawal {
                client: self.client,
                tx: self.tx,
                amount: round(self.amount),
            }),
            "dispute" => Ok(Transaction::Dispute {
                client: self.client,
                tx: self.tx,
            }),
            "resolve" => Ok(Transaction::Resolve {
                client: self.client,
                tx: self.tx,
            }),
            "chargeback" => Ok(Transaction::Chargeback {
                client: self.client,
                tx: self.tx,
            }),
            s => Err(TransactionParseError::InvalidTransactionType(s.to_owned()))
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TransactionStatus {
    Withdrew,
    Deposited,
    FailedDeposit,
    FailedWithdrawal,
    OnDispute,
    Resolved,
    Chargeback,
}

#[cfg(test)]
mod tests {
    use crate::transaction::{RawTransaction, Transaction, TransactionParseError};

    #[test]
    fn test_deposit_deserialization() {
        let raw_transaction = RawTransaction {
            transaction_type: "deposit".to_owned(),
            client: 1,
            tx: 42,
            amount: 1.0,
        };
        assert_eq!(raw_transaction.try_into(), Ok(Transaction::Deposit {
            client: 1,
            tx: 42,
            amount: 1.0,
        }))
    }

    #[test]
    fn test_withdrawal_deserialization() {
        let raw_transaction = RawTransaction {
            transaction_type: "withdrawal".to_owned(),
            client: 1,
            tx: 42,
            amount: 1.0,
        };
        assert_eq!(raw_transaction.try_into(), Ok(Transaction::Withdrawal {
            client: 1,
            tx: 42,
            amount: 1.0,
        }))
    }

    #[test]
    fn test_resolve_deserialization() {
        let raw_transaction = RawTransaction {
            transaction_type: "resolve".to_owned(),
            client: 1,
            tx: 42,
            amount: 1.0,
        };
        assert_eq!(raw_transaction.try_into(), Ok(Transaction::Resolve {
            client: 1,
            tx: 42,
        }))
    }

    #[test]
    fn test_chargeback_deserialization() {
        let raw_transaction = RawTransaction {
            transaction_type: "chargeback".to_owned(),
            client: 1,
            tx: 42,
            amount: 1.0,
        };
        assert_eq!(raw_transaction.try_into(), Ok(Transaction::Chargeback {
            client: 1,
            tx: 42,
        }))
    }

    #[test]
    fn test_dispute_deserialization() {
        let raw_transaction = RawTransaction {
            transaction_type: "dispute".to_owned(),
            client: 1,
            tx: 42,
            amount: 1.0,
        };
        assert_eq!(raw_transaction.try_into(), Ok(Transaction::Dispute {
            client: 1,
            tx: 42,
        }))
    }

    #[test]
    fn test_wrong_transaction_deserialization() {
        let raw_transaction = RawTransaction {
            transaction_type: "WRONG".to_owned(),
            client: 1,
            tx: 42,
            amount: 1.0,
        };
        let result: Result<Transaction, TransactionParseError> = raw_transaction.try_into();
        assert_eq!(result, Err(TransactionParseError::InvalidTransactionType(
            "WRONG".to_owned()
        )))
    }
}
