# Implementing MVCC and major SQL transaction isolation levels

## Introduction
本專案主要抄自 Phil Eaton 的[文章](https://notes.eatonphil.com/2024-05-16-mvcc.html)，並改寫成 java 版本。


## Isolation Level
先快速介紹一下為什麼需要 database isolation：這個東西是為了讓使用者能在資料庫高併發的情境下，對**資料一致性**與**吞吐量**間做出選擇的機制。
為了提高吞吐量，資料庫會讓多個 Transaction 同時運行，當多個 Transaction 讀寫同一份資料時，容易產生資料不一致。
而這個機制，就是讓使用者可以針對不同的業務需求，選擇不同的策略，來達到**資料一致性**與**吞吐量**的平衡。

一般主流資料庫提供了：Read Uncommitted、Read Committed、Repeatable Read、Snapshot、Serializable 五種 isolation level。
> ISO SQL-92 定義了四種隔離等級：Read Uncommitted、Read Committed、Repeatable Read、Serializable。而 Snapshot 是 SQL Server 提供的 isolation level。

## Why MVCC
上面提到資料庫為了提高吞吐量，會讓多個使用者並行的使用資料，為了避免資料不一致，我們就會如同 concurrent programming 一樣，
使用類似鎖的東西(概念上類似，但實際上是不一樣的東西)，來控制共享資源，然而在某些情境下，會產生效率不佳的問題。
因此部分的資料庫使用 [MVCC(Multiversion concurrency control)](https://zh.wikipedia.org/zh-tw/%E5%A4%9A%E7%89%88%E6%9C%AC%E5%B9%B6%E5%8F%91%E6%8E%A7%E5%88%B6) 
試圖以避免鎖的使用來提高效能，這也是本專案使用的策略。


## 實作
細節請參閱程式碼。本專案把原文章的程式碼改用 Java，實作了一個 key value store，並用簡單的資料結構來呈現概念。更多的細節請參閱[原文](https://notes.eatonphil.com/2024-05-16-mvcc.html)，
以下說明僅說明作者覺得重要的程式碼。

### 資料結構

#### Value
```java
public class Value {

	private int txStartId;  // 哪個 tx 創造了這個紀錄。由於是 MVCC 實作，insert 跟 update 相同。
	private int txEndId;  // 主要記錄哪個 tx 改動修改了紀錄，這樣就可以知道這筆紀錄是不是有效的。
	private String value;
        // ...
}
```


#### Transaction
```java
public class Transaction {

	private final IsolationLevel isolationLevel;
	private final int id;
	private TransactionState state;
	// 紀錄了當 tx 開始時，有哪些還沒結束的 tx。
	// 這個 set 主要為了處理：RepeatableRead, Snapshot, Serializable isolation level
	private final Set<Integer> inProgress;
	
	// 以下兩個 set 紀錄了當 tx 開始以後，讀、寫了哪些資料
        // 這兩個 set 幫助我們在處理 Snapshot, Serializable isolation level 時，檢查資料是否有衝突
	private final Set<String> writeSet = new HashSet<>();
	private final Set<String> readSet = new HashSet<>();

	// ...
}
```


#### Database
```java
public class Database {

	// MVCC 的精華，儲存多個版本的 value
	private Map<String, List<Value>> store;
	
	// ...
}
```

### Visibility
在 MVCC 中對同一個 key，儲存了多個版本的 value，不同的 tx 讀取到甚麼資料，主要依靠資料的可見性來運作，根據不同 isolation level，tx 能看到的版本會有所不同，實作方式為以下：

#### Read Uncommitted
可以看見所有 tx 的變動(即 Dirty Read)，因此我們只需要讓被刪除的資料不被看見。

```java
private boolean isVisible(Transaction tx, Value value) {
	IsolationLevel isolationLevel = tx.getIsolationLevel();
	if (isolationLevel == IsolationLevel.ReadUnCommitted) {
		return value.getTxEndId() == 0;  // txEndId != 0 代表這筆資料被刪除了
	}
        // ...
}
```


#### ReadCommitted
只能讀到已經 commit 的 tx，因此我們只需要讓未 commit 的 tx 不被看見。
這個 isolation level 會發生 Non-repeatable Read：
1. Tx 1: select A
2. Tx 2: update A set age = age + 1
3. Tx 2: commit
3. Tx 1: select A // A 跟第一次查詢不一樣了

```java
private boolean isVisible(Transaction tx, Value value) {
	IsolationLevel isolationLevel = tx.getIsolationLevel();
	
	// ...
	
	if (isolationLevel == IsolationLevel.ReadCommitted) {
		// 排除其他 tx 還沒 commit 的版本
		if (value.getTxStartId() != tx.getId()  // 不是自己修改的資料
				&& transactions.get(value.getTxStartId()).getState()
				!= TransactionState.Committed) {  // 被修改的資料還沒 commit
			return false;
		}

		// 資料被自己刪除了
		if (value.getTxEndId() == tx.getId()) { 
			return false;
		}
		
		// 資料被其他 tx 刪除，且這個刪除操作已經 commit
		if (value.getTxEndId() > 0 // record be deleted
                                // 修改資料的 tx 已經 commit
				&& transactions.get(value.getTxEndId()).getState()
				== TransactionState.Committed) {
			return false;
		}

		return true;
	}
}
```


### Repeatable Read、Snapshot、Serializable
解決 Read Committed 所面對的問題，這個 isolation 中不能讀到其他 tx 的資料：比我慢開始的 tx 操作我讀不到，我開始以後的 tx commit 我也看不到，因此我們需要把 tx 開始以後的操作都過濾掉。

實作上原作者會檢查 inProgress 這個 set，但少了那段程式碼，仍可以通過測試，因此推測是為了加速使用。用 inProgress set，能避免在檢查 value 時，遍歷整個 value list(不同版本的 value)。


```java
private boolean isVisible(Transaction tx, Value value) {
	IsolationLevel isolationLevel = tx.getIsolationLevel();

	// ...
  
	// 讀不到被自己刪除的資料
	if (value.getTxEndId() == tx.getId()) {
		return false;
	}

	// 比我慢開始的 tx 操作我讀不到
	if (value.getTxStartId() > tx.getId()) {
		return false;
	}

	// 忽略仍在執行的 tx
        // 這段程式碼註解以後，仍可以運行
        // 如果為了達到需求，其實不需要以下判斷，之後的邏輯仍可以刪掉不合法的區域
        // 因此這段程式碼推測試為了加速，避免讀取 value 全部的版本
	if (tx.getInProgress().contains(value.getTxStartId())) {
		return false;
	}

	// 過濾其他 tx 的操作
	if (transactions.get(value.getTxStartId()).getState() != TransactionState.Committed // tx 還沒 commit
			&& value.getTxStartId() != tx.getId()) {  // 不是自己的 tx
		return false;
	}

	// 過濾 tx 開始以前，就被刪除的資料
	if (value.getTxEndId() > 0  // 資料已經被刪除
			&& value.getTxEndId() < tx.getId()  // 比自己早開始的 tx
            // delete 操作已經 commit
			&& transactions.get(value.getTxEndId()).getState() == TransactionState.Committed
            // 資料不是在 tx 開始以後才被 commit
			&& !tx.getInProgress().contains(value.getTxEndId())  
	) {
		return false;
	}

	return true;
}
```


### Conflict
延續上面 visibility 我們可以發現 Repeatable Read、Snapshot、Serializable isolation level 在資料可見性上是相同的，他們幾個之間的差別，是當資料在 commit 階段，會有額外的檢查來保證資一致性，因此這部分會由 conflict 來更進一步區分 Repeatable Read、Snapshot、Serializable isolation level。

#### Conflict detection
Snapshot、Serializable isolation level 在 commit 階段，會檢查是否有其他 tx 讀取、修改了同一份資料，如果有，則會 rollback。
要達到目的，我們需要在 commit 階段檢查是否有衝突。由於 SnapShot、Serializable 需要檢查的內容不同，
而共同的邏輯是先檢查 commit 的 tx 在同時間有沒有其他 tx 在運作，以下為實作方式：

```java
public boolean hasOverlapTx(Transaction tx,
			// conflictFn 為深入檢查資料是否同步的 method，這部分會在個別的 isolation level 中說明
			BiFunction<Transaction, Transaction, Boolean> conflictFn) {

    // 檢查 tx 開始以後，有沒有其他 tx 也在運行
    for (Integer inProgressId : tx.getInProgress()) {
        Transaction inProgressTx = transactions.get(inprogressId);

        // 沒有同時進行的 tx
        if (inProgressTx == null) {
            continue;
        }

        // 有衝突的情況，是當其他資料有 commit 的情況下，才會有衝突
        // 所以檢查 overlap 以外，還要確認這些 tx 是不是有先 commit
        if (inProgressTx.getState() == TransactionState.Committed) {
            if (conflictFn.apply(tx, inProgressTx)) { // 發現有進行中的 tx 比自己先 commit，透過 conflictFn 進一步檢查資料是否有衝突
                return true;
            }
        }
    }

    // 確認比自己慢開始的 tx
    for (int txId = tx.getId(); txId < nextTransactionId; txId++) {
        Transaction afterTx = transactions.get(txId);
        if (afterTx == null) {  // 沒有這個 tx
            continue;
        }

        // 當比自己慢開始的 tx 比自己先 commit，檢查是否有資料衝突
        if (afterTx.getState() == TransactionState.Committed) {
            if (conflictFn.apply(tx, afterTx)) {
                return true;
            }
        }
    }

    return false;
}
```






#### Repeatable Read
不會進行 conflict 檢查。關於 visible 都是關於能否看到資料，但對於寫入同一個資料，卻沒有檢查，因此可能出現：
1. Tx 1 update A set age = 10
2. Tx 2 update A set age = 20
3. Tx 1 commit
4. Tx 2 commit
5. 兩個 Tx commit 都會成功，A.age = 20

上面的情況，是另外兩個 isolation level 不允許發生的。


#### Snapshot
在 commit 階段，會檢查是否有其他 tx 修改了同一份資料，如果有，則會 rollback。
以上面 Repeatable Read 提到的情況，Tx 2 會 rollback 並告訴使用者 commit 失敗(Abort)。

```java
public void completeTransaction(Transaction transaction, TransactionState state) {
	if (state == TransactionState.Committed) {
		
		// ...
		
		// isolation level 為 snapshot
		if (transaction.getIsolationLevel() == IsolationLevel.Snapshot
				// 檢查執行中的 tx 是否有寫到同一份資料 
				&& hasOverlapTx(transaction,
				// 確認同時執行的 tx，有沒有寫到同一筆資料
				(currentTx, overlapTx) -> checkSharedValueConflict(currentTx.getWriteSet(),
						overlapTx.getWriteSet()))) {
			// 有寫到同一筆資料，這次操作 Abort(rollback)
			completeTransaction(transaction, TransactionState.Aborted);
			// 告訴 client 有 conflict
			throw new RuntimeException("write-write conflict");
		}

	}
    // ...
}
```

#### Serializable
使用上會像是一條 tx 執行完，再換下一個 tx 執行，然而實際執行時，不可能真的這樣做，
所以實作上，是當資料衝突時，rollback 並告知 client 有衝突，針對衝突的情況有：
* 我讀的資料有別人修改過
* 我修改到別人讀取的資料

```java
public void completeTransaction(Transaction transaction, TransactionState state) {
	if (state == TransactionState.Committed) {
		
		// ...
      
		// isolation level 為 Serializable
		if (transaction.getIsolationLevel() == IsolationLevel.Serializable && (
				// 檢查衝突
				hasOverlapTx(transaction,
						// 我讀的資料有別人修改過
						(currentTx, overlapTx) -> checkSharedValueConflict(currentTx.getReadSet(),
								overlapTx.getWriteSet()))
						|| hasOverlapTx(transaction,
						// 我修改到別人讀取的資料
						(currentTx, overlapTx) -> checkSharedValueConflict(currentTx.getWriteSet(),
								overlapTx.getReadSet()))
              )
		) {
			completeTransaction(transaction, TransactionState.Aborted);
			throw new RuntimeException("read-write conflict");
		}
	}
	// ...
}
```



## 其他
如原文所述，這個實作選擇了簡單的概念來實作，實際上不同資料庫有不同的實作方式，絕對比現在的版本複雜很多。
而在真實世界中，MVCC 因為保存了多個版本的資料，所以會需要額外進行資料清理，又叫做 ```Vacuuming```。
所以本專案只能用在熟悉資料庫 isolation level，並讓人對 MVCC 有初步的瞭解的練習小專案。