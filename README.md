# Coding test

My solution to the coding test.

## Problem assumptions

1. A frozen account can receive deposits, but not withdrawals.
2. There can't be more than one dispute on a given transaction.
The program ignores subsequent disputes.

## How it works

The solution processes each client in parallel inside a thread 
pool. Given that they have no shared data, they can run efficiently
in their own process without worrying about communication and 
locks.

The algorithm is something like:

```
for each transaction in the csv
  if the client of the transaction doesn't have a process
    create a channel to communicate with the process
    start a process for the client
  send the transaction to the process of the client
```

The process of the client will aggregate the data one transaction
at the time until the program closes its channel. At the end, it
will send back its result.

If there is an error in a particular transaction, it will add the
error to a running list of errors and continue, so that users can
see all the problems on the first run. When possible, it will give
meaningful messages on what is the problem with the data.

Errors concerning broken assumptions, like poisoned mutexes, will
halt the program all together.

The core logic is in an isolated library, allowing for the 
creation of a different frontend than the CLI.

## Performance

The solution will run a number of threads equal to the number of 
processes in the host. This should allow for an efficient use of 
cpu resources.

The runtime performance is `O(n)`, where `n` is the number of 
transaction operations.

Each client process contains an internal record of the 
transactions it saw so far. Thus, the memory complexity is 
`O(m)` where m is the number of unique transaction ids.

The solution processes the data as a stream.