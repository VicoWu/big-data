- [Revoke Memory Basic Strategy：](#revoke-memory-basic-strategy-)
- [Memory Revoking Scheduling](#memory-revoking-scheduling)
  * [How Memory Revoking is Triggered](#how-memory-revoking-is-triggered)
    + [Triggered by Registered Listener](#triggered-by-registered-listener)
    + [Triggered by Periodical Scheduling](#triggered-by-periodical-scheduling)
    + [Revoking Condition and Revoking Memory Amount](#revoking-condition-and-revoking-memory-amount)
  * [How Memory Revoking is Triggered](#how-memory-revoking-is-triggered-1)
- [Operators](#operators)
  * [HashBuilderOperator](#hashbuilderoperator)
    + [Unspill the Lookup Source](#unspill-the-lookup-source)
    + [Partition Count Decision](#partition-count-decision)
  * [LookupJoinOperator](#lookupjoinoperator)
    + [Spiller for LookupJoinOperator: GenericPartitioningSpiller](#spiller-for-lookupjoinoperator--genericpartitioningspiller)
    + [Spilling Mask for HashBuildOperator and LookupJoinOperator](#spilling-mask-for-hashbuildoperator-and-lookupjoinoperator)
    + [Try to Unspill the LookupSource](#try-to-unspill-the-lookupsource)
    + [HashGenerator for Probe](#hashgenerator-for-probe)
  * [Spilling in Window](#spilling-in-window)
  * [Spilling in Sort](#spilling-in-sort)
  * [Spilling In Aggregation](#spilling-in-aggregation)
  * [**SingleStreamSpillerFactory**](#--singlestreamspillerfactory--)
  * [**PartitioningSpillerFactory**](#--partitioningspillerfactory--)
- [References](#references)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>


# Revoke Memory Basic Strategy：

This is what from PrestoSQL's official doc:

By default, Presto kills queries if the memory requested by the query execution exceeds session properties `query_max_memory` or `query_max_memory_per_node`. This mechanism ensures fairness in allocation of memory to queries and prevents deadlock caused by memory allocation. It is efficient when there is a lot of small queries in the cluster, but leads to killing large queries that don’t stay within the limits.

To overcome this inefficiency, the concept of revocable memory was introduced. A query can request memory that does not count toward the limits, but this memory can be revoked by the memory manager at any time. When memory is revoked, the query runner spills intermediate data from memory to disk and continues to process it later.

In practice, when the cluster is idle, and all memory is available, a memory intensive query may use all of the memory in the cluster. On the other hand, when the cluster does not have much free memory, the same query may be forced to use disk as storage for intermediate data. A query that is forced to spill to disk may have a longer execution time by orders of magnitude than a query that runs completely in memory.

Please note that enabling spill-to-disk does not guarantee execution of all memory intensive queries. It is still possible that the query runner will fail to divide intermediate data into chunks small enough that every chunk fits into memory, leading to `Out of memory` errors while loading the data from disk.



This is the commit which is used to introduce the revokable memory feature: https://github.com/prestodb/presto/commit/ce0716ac8a1528c9f19e52b7b02bb477736859ea 



Official introduction for Presto Spill to Disk https://prestodb.io/docs/current/admin/spill.html



# Memory Revoking Scheduling

This is the PR [Introduce Revokable User Memory Concept](https://github.com/prestodb/presto/pull/7895) and the corresponding git revision [ce0716ac](https://github.com/prestodb/presto/commit/ce0716ac8a1528c9f19e52b7b02bb477736859ea)  which introduced the memory revoking and added an interface in interface `Operator` which is the parent of all the operator implements in Presto:

It added an method in the `Operator` interface named 

```java
/**
 * After calling this method operator should revoke all reserved revocable memory.
 * As soon as memory is revoked returned future should be marked as done.
 * <p>
 * Spawned threads cannot modify OperatorContext because it's not thread safe.
 * For this purpose implement {@link #finishMemoryRevoke()}
 * <p>
 * Since memory revoking signal is delivered asynchronously to the Operator, implementation
 * must gracefully handle the case when there no longer is any revocable memory allocated.
 * <p>
 * After this method is called on Operator the Driver is disallowed to call any
 * processing methods on it (isBlocked/needsInput/addInput/getOutput) until
 * {@link #finishMemoryRevoke()} is called.
 */
default ListenableFuture<?> startMemoryRevoke()
{
    return NOT_BLOCKED;
}
```



## How Memory Revoking is Triggered

The memory revoking is triggered in below 2 circumstances:

1. A scheduled periodical check
2. The memory pool has changed

An atomic value is used to confirm that only one revoking request will be triggered

### Triggered by Registered Listener

All above is defined in `MemoryRevokingScheduler`

```java
public class MemoryRevokingScheduler
{
    ....
    private final MemoryPoolListener memoryPoolListener = MemoryPoolListener.onMemoryReserved(this::onMemoryReserved);
```

This listener will be registered to the memory pool when the pool is started.

```java
@VisibleForTesting
void registerPoolListeners()
{
    memoryPools.forEach(memoryPool -> memoryPool.addListener(memoryPoolListener));
}
```

We could check the code of MemoryPool and find that this listener will be triggered when methods like `reserve()`, `reserveRevocable()`, `tryReserve()` are called.

### Triggered by Periodical Scheduling

When Presto server started and the injection finished, the method `start()` will be called;

> The The PostConstruct annotation is used on a method that needs to be executed after dependency injection is done to perform any initialization.

```java
@PostConstruct
public void start()
{
    registerPeriodicCheck();
    registerPoolListeners();
}

private void registerPeriodicCheck()
{
    this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
        try {
            requestMemoryRevokingIfNeeded();
        }
        catch (Throwable e) {
            log.error(e, "Error requesting system memory revoking");
        }
    }, 1, 1, SECONDS);
}
```

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/50562f4201e42c6c72a3820ebfa0fadcf52341d7/src/main/resources/images/presto/memory_revoking_scheduling.png" alt="canRunMore" width="1000" />



**memory-revoking-threshold**: Revoke memory when memory pool is filled over threshold. Default is `0.9`

**memory-revoking-target**:       When revoking memory, try to revoke so much that pool is filled below target at the end. 

​                                                       Default is `0.5`





```java
    @GuardedBy("exclusiveLock")
    private void handleMemoryRevoke()
    {
        for (int i = 0; i < activeOperators.size() && !driverContext.isDone(); i++) {
            Operator operator = activeOperators.get(i);

            if (revokingOperators.containsKey(operator)) {
                checkOperatorFinishedRevoking(operator);
            }
            else if (operator.getOperatorContext().isMemoryRevokingRequested()) {
                // Create the future that start the memory revoking
                ListenableFuture<?> future = operator.startMemoryRevoke();
                // Put the revoking future into management
                revokingOperators.put(operator, future);
                checkOperatorFinishedRevoking(operator);
            }
        }
    }
```

### Revoking Condition and Revoking Memory Amount

So, from above analysis, we know that revoking will be trigger when memory pools' memory is reserved or by a periodical scheduling. But it doesn't mean that everytime when listener is triggered or periodical scheduling arrived, we will revoke memory.

So, we still have 2 questions to resolve:

1. When revoking opportunity arrived, whether or not revoking is necessary current
2. If revoking is necessary, how much memory should be revoked?

Let's answer above questions from below code analysis:

When `runMemoryRevoking` is called, it will firstly check the `memoryRevoking` is necessary in current memory circumstances:

```java
private synchronized void runMemoryRevoking()
{
    if (checkPending.getAndSet(false)) {
        Collection<SqlTask> sqlTasks = null;
        for (MemoryPool memoryPool : memoryPools) {
            if (!memoryRevokingNeeded(memoryPool)) {
                continue;
            }
            requestMemoryRevoking(memoryPool, sqlTasks);
        }
    }
}
```

```java
// Whether memory revoking is necessary
private boolean memoryRevokingNeeded(MemoryPool memoryPool)
{
    // If the memory pool has revocable bytes and the free bytes of this pool is less than the revoking threshold
    return memoryPool.getReservedRevocableBytes() > 0
            && memoryPool.getFreeBytes() <= memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold);
}
```
`memoryRevokingThreshold` is configured by `memory-revoking-threshold` whose default value is 0.9, whic indicates that we will try to revoke memory when memory pool is filled over this threshold

For example, when we configured `memory-revoking-threshold` as `0.9` and the memory pool capacity is `100GB`, then when and only when the pool has revocable memory and the free bytes of the memory pool is less than `100G * (1 - 0.9) = 10G`



So, the next question, when the memory revoking condition is met, how much memory should be revoked?

```java
private void requestMemoryRevoking(MemoryPool memoryPool, Collection<SqlTask> sqlTasks)
{

    long remainingBytesToRevoke = (long) (-memoryPool.getFreeBytes() + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
    remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(sqlTasks, memoryPool);
    requestRevoking(memoryPool, sqlTasks, remainingBytesToRevoke);
}
```

From above code, we knows that the memory to be revoked is calculated as:
`memoryRevokingTarget` is a configurable value(`memory-revoking-target`, default if 0.5), which indicates the target that the pool is filled after revoking. 

For example, when target is `0.7`, it means that we will try to revoke memory until the pool is filled with `70%`. For example, when pool is `100G`, memoryRevokingTarget is `0.7`, current free memory is `20G`, then remaining bytes to revoke will be `100G * (1 - 0.7) - 20G = 10G`. Currently there is already `2G` memory revoked, so the remainingBytesToRevoke will be `10G - 2G = 8G`



After the revoking bytes is calculated, the `MemoryPool` will traverse all the tasks and try to revoke the target bytes memory

```java
private void requestRevoking(MemoryPool memoryPool, Collection<SqlTask> sqlTasks, long remainingBytesToRevoke)
{
    AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
    sqlTasks.stream()
            .filter(task -> task.getTaskStatus().getState() == TaskState.RUNNING)
            .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
            .sorted(ORDER_BY_CREATE_TIME)
            .forEach(task -> task.getQueryContext().accept(new VoidTraversingQueryContextVisitor<AtomicLong>()
            {
                @Override
                public Void visitQueryContext(QueryContext queryContext, AtomicLong remainingBytesToRevoke)
                {
                    if (remainingBytesToRevoke.get() < 0) {
                        // exit immediately if no work needs to be done
                        return null;
                    }
                    return super.visitQueryContext(queryContext, remainingBytesToRevoke);
                }

                @Override
                public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                {
                    if (remainingBytesToRevoke.get() > 0) {
                        long revokedBytes = operatorContext.requestMemoryRevoking();
                        if (revokedBytes > 0) {
                            remainingBytesToRevoke.addAndGet(-revokedBytes);
                            log.debug("memoryPool=%s: requested revoking %s; remaining %s", memoryPool.getId(), revokedBytes, remainingBytesToRevoke.get());
                        }
                    }
                    return null;
                }
            }, remainingBytesToRevokeAtomic));
}
```

​    

After the revoking request is fired, then  , when the operator is running, it will try to check the revoking request flag and try to revoke memory:

```java
@GuardedBy("exclusiveLock")
private ListenableFuture<?> processInternal(OperationTimer operationTimer)
{
    handleMemoryRevoke();
```

```java
@GuardedBy("exclusiveLock")
private void handleMemoryRevoke()
{
    for (int i = 0; i < activeOperators.size() && !driverContext.isDone(); i++) {
        Operator operator = activeOperators.get(i);
        //Check whether the memory revoking is already running
        if (revokingOperators.containsKey(operator)) {
            checkOperatorFinishedRevoking(operator);
        }
        else if (operator.getOperatorContext().isMemoryRevokingRequested()) {
            // Create the future that start the memory revoking
            ListenableFuture<?> future = operator.startMemoryRevoke();
            revokingOperators.put(operator, future);
            //block waiting until memory revoking finished
            checkOperatorFinishedRevoking(operator);
        }
    }
}
```

From the `handleMemoryRevoke()`,we could see that it will traverse all the operators in this driver and check one by one:

- If this operator is already doing the revoking, check whether the revoking is done. If done, reset the revoking request
- If this operation is not doing the revoking, call `startMemoryRevoke()` on this operator to do the memory revoking

Firstly, let's check the details of `checkOperatorFinishedRevoking()`:

```java
@GuardedBy("exclusiveLock")
private void checkOperatorFinishedRevoking(Operator operator)
{
    // Get the revoking operator
    ListenableFuture<?> future = revokingOperators.get(operator);
    if (future.isDone()) { // Revoking is finished
        getFutureValue(future); // propagate exception if there was some
        revokingOperators.remove(operator);
        // Call the implements of the finishMemoryRevoke
        operator.finishMemoryRevoke();
        // Reset the memory revoking request
        operator.getOperatorContext().resetMemoryRevokingRequested();
    }
}
```



Then the `startMemoryRovoke()` of detailed Operator implements will be called.

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/0609cd7367ec166bc3dc5469d42609fec5ef4077/src/main/resources/images/presto/startMemoryRevoke.png" alt="canRunMore" width="1000" />

Check the implements of interface method `startMemoryRevoke()`, we could clearly find that what opeartion implements memory revoking.



## How Memory Revoking is Triggered

For the `finishMemoryRevoke()` which is called in `Driver.checkOperatorFinishedRevoking()` we introduced in previous chapte

r:

```java
@Override
public void finishMemoryRevoke()
{
    checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
    finishMemoryRevoke.get().run();
    finishMemoryRevoke = Optional.empty();
}
```
This is call stacktrace that the `finishMemoryRevoke()` will be triggered and the variable `finishMemoryRevoke` will be set to empty

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/39c4f8a5999ed6da81b23b73bf051c4c4dcc4a9b/src/main/resources/images/presto/finishMemoryRevoking_stack.png" alt="canRunMore" width="1000" />



When the `Operator.finish()` is called, then there will be state change. 

> ```java
> /**
>  * Notifies the operator that no more pages will be added and the
>  * operator should finish processing and flush results. This method
>  * will not be called if the Task is already failed or canceled.
>  */
>  void finish();
> ```



Let's check the implements of `HashBuilderOperator.finish()`:

```java

    @Override
    public void finish()
    {
        ....
        // We will do nothing is finishMemoryRevoke is present, it proves that we are still revoking memory
        // and the finishMemoryRevoke() is not called yet.
        // The finishMemoryRevoke() will be triggered when Driver.process() finds that the memory revoking is done, and then
        // variable finishMemoryRevoke will be set to empty
        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput(); // The state will be switched to LOOKUP_SOURCE_BUILT
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested(); // The state will be CLOSED
                return;

            case SPILLING_INPUT:
                // State will changed from SPILLING_INPUT to INPUT_SPILLED if the spilling is not finished yet
                // Other wise , the state will still be SPILLING_INPUT
                finishSpilledInput();
                return;

            case INPUT_SPILLED:
                if (spilledLookupSourceHandle.getDisposeRequested().isDone()) {
                    close(); // State will be CLOSED
                }
                else {
                    unspillLookupSourceIfRequested(); // state will be INPUT_UNSPILLING
                }
                return;

            case INPUT_UNSPILLING:
                finishLookupSourceUnspilling(); // state will be INPUT_UNSPILLED_AND_BUILT
                return;

            case INPUT_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested(); // state will be CLOSED
                return;
            ....
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

```

1. When `finishMemoryRevoke` is present, which indicates that the memory revoking is still in progress, we will do nothing
2. When `finishMemoryRevoke` is not present(Method `finishMemoryRevoke()` is triggered by `Driver`) and the state is `SPILLING_INPUT`, then the state will be switched to `INPUT_SPILLED`



# Operators

The factory for spilling is abstracted in interface `SpillerFactory`

```java
public interface SpillerFactory
{
    Spiller create(List<Type> types, SpillContext localSpillContext, AggregatedMemoryContext aggregatedMemoryContext);
}
```

The spiller factory construction is in `ServerMainModule`:

```java
// Spiller
binder.bind(SpillerFactory.class).to(GenericSpillerFactory.class).in(Scopes.SINGLETON);
binder.bind(SingleStreamSpillerFactory.class).to(FileSingleStreamSpillerFactory.class).in(Scopes.SINGLETON);
binder.bind(PartitioningSpillerFactory.class).to(GenericPartitioningSpillerFactory.class).in(Scopes.SINGLETON);
```



So, in the `LocalExecutionPlanner`, we know the corresponding implement class of different SpillFactory:

```java
@Inject
public LocalExecutionPlanner(
        ......
        SpillerFactory spillerFactory, // Implements is GenericSpillerFactory
        SingleStreamSpillerFactory singleStreamSpillerFactory,  // Implements is FileSingleStreamSpillerFactory
        PartitioningSpillerFactory partitioningSpillerFactory, // Implements is GenericPartitioningSpillerFactory
        .......)
{
```

The corresponding PR for the aggregation spilling to disk design: https://github.com/prestodb/presto/commit/a5ccc7236148118d6b316a1fbb1ad4229e7dded9

For LookupJoinOperator

```
public class LookupJoinOperatorFactory
        implements JoinOperatorFactory, AdapterWorkProcessorOperatorFactory
```



## HashBuilderOperator

Below is the UML for the `HashBuilderOperator` which is responsible for the build side data management:

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/b5ce43c5bf64f6d4d049c416a97d9f8a8ded2e47/src/main/resources/images/presto/spill-to-disk/HashBuilderOperator_UML.png" alt="canRunMore" width="300" align="left" />



We could see that the `HashBuilderOperator` implements the `startMemoryRevoke()` and `finishMemoryRevoke()`, which means that, once the memory revoking condition meets,  `HashBuilderOperator` will do the memory rovoke in the build side. And at the same time, the whole join logic will change after spill is triggered, for example, when the the probe side find that the build side has been spilled, then the join of cource cannot proceed anymore, and the thus the probe side will also be spilled.



 Below is the basic procedure for the build side spilling:

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/master/src/main/resources/images/presto/spill-to-disk/Join%20Spilling%20Architecture.png" alt="canRunMore" width="1200" align="left" />



**SingleStreamSpillerFactory** is used in `HashBuilderOperator`:

```java
private JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(
        JoinNode node,
        LocalExecutionPlanContext context,
        boolean spillEnabled)
{
    LocalExecutionPlanContext buildContext = context.createSubContext();
    PhysicalOperation buildSource = buildNode.accept(this, buildContext);
    .....
    HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
            .....
            spillEnabled && !buildOuter && partitionCount > 1,
            singleStreamSpillerFactory);
```

The initial state of `HashBuilderOperator` is `CONSUMING_INPUT`. In this state, 

Let's check the implements of method `startMemoryRevoke()` which overrides the interface `Operator`. From the method `handleMemoryRevoke()`, we could see that when this method is called, we are sure that the memory revoking is necessary(Some threashold has been reached)



`LookupSourceFactory` is created inside `LocalExecutionPlanner` and thus there should only one `LookupSourceFactory` locally for each query. But since for Join and Aggregation, there should exist a concurrency which is setup by `task.concurrency`, so the `PartitionedLookupSourceFactory` has a `partitions` to store the partition in the look up side. 

> We need to differenciate the LookupSource partitions and the probe side partitions
>
> - The look up side partitions are decided by the Presto concurrency decided by  `task.concurrency`
> - The probe side partitions are decided by the hash policy of the probe streaming data



Each `HashBuilderOperator` is constructed with a specified index, we there are totally `partitions.length()` partitions and for each partiton, a dedicated `HashBuilderOperator` object will be created.

All the `PartitionedLookupSource`  will be created for each partition; One `PartitionedLookupSourceFactory`  will manage all the `PartitionedLookupSource`.



For unspilled partitions, `HashBuilderOperator.finishInput()` will call `lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition)`.

For spilled partitions,  `HashBuilderOperator.startMemoryRevoke()` -> `lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);` -> `partitions[partitionIndex] = () -> spilledLookupSource;`



```java
@Override
public ListenableFuture<?> startMemoryRevoke()
{
    if (state == State.CONSUMING_INPUT) {
        long indexSizeBeforeCompaction = index.getEstimatedSize().toBytes();
        index.compact();
        long indexSizeAfterCompaction = index.getEstimatedSize().toBytes();
        if (indexSizeAfterCompaction < indexSizeBeforeCompaction * INDEX_COMPACTION_ON_REVOCATION_TARGET) {
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateFuture(null);
        }

        finishMemoryRevoke = Optional.of(() -> {
            index.clear();
            localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
            localRevocableMemoryContext.setBytes(0);
            // Will increase the spilling epoch
            lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
            // As long as the state is SPILLING_INPUT, the new input will continue to spill
            // The SPILLING_INPUT will be switched to INPUT_SPILLED when the Operator.finish() is called. 
            state = State.SPILLING_INPUT;
        });
        return spillIndex();
    }
    if (state == State.LOOKUP_SOURCE_BUILT) {
        finishMemoryRevoke = Optional.of(() -> {
            lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
            lookupSourceNotNeeded = Optional.empty();
            index.clear();
            localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
            localRevocableMemoryContext.setBytes(0);
            lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
            lookupSourceSupplier = null;
            state = State.INPUT_SPILLED;
        });
        return spillIndex();
    }
    if (operatorContext.getReservedRevocableBytes() == 0) {
        // Probably stale revoking request
        finishMemoryRevoke = Optional.of(() -> {});
        return immediateFuture(null);
    }
}
```

We could see that the memory context information of this Operator is changed according to the estimated memory after spilling. And of course, the  memory context will be updated again if we are trying to unspill the build side data again.

> We will find later that for the probe side, the memory is not even considered because the probe side data is streamed in. Even when unspilling, the probe side data is also streamedly unspilled. So the probe side memory consumption is always insignificant

`PartitionedLookupSourceFactory.setPartitionSpilledLookupSourceHandle()` is the factory to maintain the spilled partition snapshot information, for example, the spilled partitions. The spilled partitions will be made to a mask. 



```java
public void setPartitionSpilledLookupSourceHandle(int partitionIndex, SpilledLookupSourceHandle spilledLookupSourceHandle)
{
    requireNonNull(spilledLookupSourceHandle, "spilledLookupSourceHandle is null");

    boolean completed;

    lock.writeLock().lock();
    try {
        if (destroyed.isDone()) {
            spilledLookupSourceHandle.dispose();
            return;
        }

        checkState(!spilledPartitions.containsKey(partitionIndex), "Partition already set as spilled");
        // Setup the spilled partition mask information
        spilledPartitions.put(partitionIndex, spilledLookupSourceHandle);
        spillingInfo = new SpillingInfo(spillingInfo.spillEpoch() + 1, spilledPartitions.keySet());

        if (partitions[partitionIndex] != null) {
            // Was present and now it's spilled
            completed = false;
        }
```

 

The `spillIndex()` will return a `Future` in which the memory revoking will be conducted. From the following chapter, we will know that this future will be put to `Driver` and Driver will check the Future status and make sure it is finished or not.



The `spillIndex()` is the true place to revoke the memory **SingleStreamSpillerFactory**

```java
private ListenableFuture<?> spillIndex()
{
    checkState(!spiller.isPresent(), "Spiller already created");
    // Create the spiller according to the spilling factory
    spiller = Optional.of(singleStreamSpillerFactory.create(
            index.getTypes(),
            operatorContext.getSpillContext().newLocalSpillContext(),
            operatorContext.newLocalSystemMemoryContext(HashBuilderOperator.class.getSimpleName())));
    return getSpiller().spill(index.getPages());
}
```



In the `spillIndex`, we could see that we will create a `FileSingleStreamSpiller` instance for each operator instance; When `FileSingleStreamSpiller` is constructed, the target file is decided. 

```java
@Override
public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
{
    Optional<SpillCipher> spillCipher = Optional.empty();
    if (spillEncryptionEnabled) { // create spill cipher if encryption is enforced
        spillCipher = Optional.of(new AesSpillCipher());
    }
    PagesSerde serde = serdeFactory.createPagesSerdeForSpill(spillCipher);
    return new FileSingleStreamSpiller(serde, executor, getNextSpillPath(), spillerStats, spillContext, memoryContext, spillCipher);
}
/**
* We choose next available spill path by round-robin style
*/
private synchronized Path getNextSpillPath()
{
    int spillPathsCount = spillPaths.size();
    for (int i = 0; i < spillPathsCount; ++i) {
        int pathIndex = (roundRobinIndex + i) % spillPathsCount;
        Path path = spillPaths.get(pathIndex);
        if (hasEnoughDiskSpace(path) && spillPathHealthCache.getUnchecked(path)) {
            roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
            return path;
        }
    }
    if (spillPaths.isEmpty()) {
        throw new PrestoException(OUT_OF_SPILL_SPACE, "No spill paths configured");
    }
    throw new PrestoException(OUT_OF_SPILL_SPACE, "No free or healthy space available for spill");
}
```

Since the spiller implements for spiller factory `SingleStreamSpillerFactory` is `FileSingleStreamSpiller`

> Once the `FileSingleStreamSpiller` is created, then the spiller is hold by this `Operator`. So for the following spilling and unspilling, the same spiller will be used based on the same spilling path, which avoid the case that one task restores the spilled files of other tasks

This is `FileSingleStreamSpiller.spill()`: 

```java
@Override
public ListenableFuture<?> spill(Iterator<Page> pageIterator)
{
    requireNonNull(pageIterator, "pageIterator is null");
    checkNoSpillInProgress();
    spillInProgress = executor.submit(() -> writePages(pageIterator));
    return spillInProgress;
}
```

Also, from the definition of spill(), we could see that it will spill once for a IndexPage which contains a batch of pages. So, the spilling will do the spill once for a batch of pages instead of a single page.

### Unspill the Lookup Source

```java
LookupSourceSupplier partition = buildLookupSource();
lookupSourceChecksum.ifPresent(checksum ->
        checkState(partition.checksum() == checksum, "Unspilled lookupSource checksum does not match original one"));
localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());

spilledLookupSourceHandle.setLookupSource(partition);
```

After unspilled , the lookup source information is stored in the `HashBuilderOperator` as the `spilledLookupSourceHandle` . This hash will be later used inside the `LookupJoinOperator` for the `LookupJoin` processing.



### Partition Count Decision

We now knows that since the build side table will  fit in memory while the probe side table could be processed row by row, so, the true memory top consumer should be the build side table. Presto will divide the probe side table to several partitions and try to do the spill partition by partition.

Let's check the code of `LocalExecutionPlanner`: 

```java
// Compute the partition count 
int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildSource.getLayout(),
                            context.getTypes(),
                            context.getSession()));

            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    buildOuter,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new PartitionedLookupSourceFactory(
                            buildSource.getTypes(),
                            buildOutputTypes,
                            buildChannels.stream()
                                    .map(buildSource.getTypes()::get)
                                    .collect(toImmutableList()),
                            partitionCount, //  The partition count
                            buildOuter),
                    buildOutputTypes);

```

From above code, we could see that the partition count is decided by the `driverInstanceCount` when building the local query plan. Then, how is `driverInstanceCount` decided?  In `LocalExecutorPlanner`, we could see very clearly about the partitionCount decision:

```java
private PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
{
    int driverInstanceCount;
    if (node.getType() == ExchangeNode.Type.GATHER) {
        driverInstanceCount = 1;
        context.setDriverInstanceCount(1);
    }
    else if (context.getDriverInstanceCount().isPresent()) {
        driverInstanceCount = context.getDriverInstanceCount().getAsInt();
    }
    else {
        // If the driverInstanceCount is not decided, it is decided by the task_concurrency configuration
        driverInstanceCount = getTaskConcurrency(session);
        context.setDriverInstanceCount(driverInstanceCount);
    }
```

So, the size of partitions is decided by the configured `task_concurrency` session property and configured `task.concurreny` cluster config. So, the number of partition count for build side table is in fact a static value.





## LookupJoinOperator

Below is the  `LookupJoinOperator` UML. We could find that the `LookupJoinOperator` is an indirect implements of interface `Operator`.

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/b5ce43c5bf64f6d4d049c416a97d9f8a8ded2e47/src/main/resources/images/presto/spill-to-disk/LookupJoinOperator_uml.png" alt="canRunMore" width="300"  align="left" />



 We could see that `LookupJoinOperator` and `WorkProcessorOperatorAdapter` didn't implements `startMemoryRevoke()`, which means that when the memory revoking triggered, it has nothing to do with the `LookupJoinOperator`. Then, you may ask, how the spill to disk happen if the `LookupJoinOperator` didn't handle the `startMemoryRevoke()`?

In fact, only the build side operator, `HashBuilderOperator` implements the `startMemoryRevoke()`. When this memory is triggered, then there will be a mask map indicates which partition is spilled or which is not. Then, in the `LookupJoinOperator ` side, it will decide the page processing logic according to the build side spilling status:

-  If the partition of this probe side row is already spilled in the build side, then the corresponding probe side will also be spilled.
-  If the partition of this probe side row is still in memory, then the corresponding probe side will be processed as normal



Below is the basic procedure for the probe side spilling architecture:

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/master/src/main/resources/images/presto/spill-to-disk/Probe%20Side%20Spilling.png" alt="canRunMore" width="1200" align="left" />

### Spiller for LookupJoinOperator: GenericPartitioningSpiller

The spiller used by `HashBuilderOperator` is the `GenericPartitioningSpiller` which is created by `GenericPartitioningSpillerFactory`.

Check the code of `GenericPartitioningSpiller.getSpiller()`, we could find that the `GenericPartitioningSpiller` is just a wrapper for the `SingleStreamSpiller()`.

```java
private final List<Optional<SingleStreamSpiller>> spillers; // The spillers for each partition
```

```java
private synchronized SingleStreamSpiller getSpiller(int partition)
{
    Optional<SingleStreamSpiller> spiller = spillers.get(partition);
    // If the spiller is not present, we will create a apiller for it
    if (!spiller.isPresent()) {
        // spillerFactor implements is FileSingleStreamSpillerFactory.
        // HashBuilder Operator is also using FileSingleStreamSpillerFactory and
        spiller = Optional.of(closer.register(spillerFactory.create(types, spillContext, memoryContext.newLocalMemoryContext(GenericPartitioningSpiller.class.getSimpleName()))));
        spillers.set(partition, spiller);
    }
    return spiller.get();
}
```

For each page, it will firstly get the partition of this `Page` and then try to spill this page.

Firstly it will call `partitionPage()` to get the partition for each row and return the unspilled position index:

```java
private synchronized IntArrayList partitionPage(Page page, IntPredicate spillPartitionMask)
{
    IntArrayList unspilledPositions = new IntArrayList();

    // For each position of this page(Please remember that we could think of position as the row index)
    for (int position = 0; position < page.getPositionCount(); position++) {
        // Get the partition value of current position
        int partition = partitionFunction.getPartition(page, position);

        // The mask test didn't pass, we will not spill this partiton
        if (!spillPartitionMask.test(partition)) {
            unspilledPositions.add(position);
            continue;
        }
        // This is a spilled partition
        spilledPartitions.add(partition);

        // Get the page builder of this partition
        PageBuilder pageBuilder = pageBuilders.get(partition);
        pageBuilder.declarePosition();
        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
        }
    }

    return unspilledPositions;
}
```



After that, the`flushFullBuilders()` will be called to flush the page:

```java
private ListenableFuture<?> flushFullBuilders()
{
    // Flush only when PageBuilder::isFull returns true
    return flush(PageBuilder::isFull);
}
```
Let's check the detail of `flush()`:
```java
private synchronized ListenableFuture<?> flush(Predicate<PageBuilder> flushCondition)
{
    requireNonNull(flushCondition, "flushCondition is null");
    ImmutableList.Builder<ListenableFuture<?>> futures = ImmutableList.builder();

    //Each partition will have a PageBuilder and a Spiller(Not each position)
    for (int partition = 0; partition < spillers.size(); partition++) {
        PageBuilder pageBuilder = pageBuilders.get(partition);
        // The flushCondition is PageBuilder::isFull, so only when page is full, we start to flush
        if (flushCondition.test(pageBuilder)) {
            // we need to flush this partition of this page
            futures.add(flush(partition));
        }
    }

    return Futures.allAsList(futures.build());
}
```

```java
// Flush only the partiton data
private synchronized ListenableFuture<?> flush(int partition)
{
    // Get the pagebuilder of this partition
    PageBuilder pageBuilder = pageBuilders.get(partition);
    if (pageBuilder.isEmpty()) {
        return Futures.immediateFuture(null);
    }
    Page page = pageBuilder.build(); // The page is already the page of a partition
    pageBuilder.reset();
    // Each partition will have a dedicated spiller, which should be FileSingleStreamSpillerFactory.spill()
    return getSpiller(partition).spill(page);
}
```

We could see that the flush is operated asynchorously.

Since we know that for each page, only some positions(row) will be flushed. By `Page page = pageBuilder.build();`, it will build a new page from current page which contains only the rows which need to be partitioned.



We should notice that `LookupJoinOperator` is not implement of interface `Operator`, it is just implement of `AdapterWorkProcessorOperator`

```java
// This is not a Operator implements
public interface AdapterWorkProcessorOperator
        extends WorkProcessorOperator
{
    boolean needsInput();

    void addInput(Page page);

    void finish();
}
```



### Spilling Mask for HashBuildOperator and LookupJoinOperator

The change of the spill mask is triggered from the `HashBuilderOperator.startMemoryRevoke()`, it is easy to understand: When the build side table have to spill, we have to construct such a mask table to record which build partiton has been spilled or which is not.

We could see that when `HashBuilderOperator` is constructed, the `PartitionedLookupSourceFactory` object is transfered to it:

```java
public HashBuilderOperator(
        OperatorContext operatorContext,
        PartitionedLookupSourceFactory lookupSourceFactory,
        int partitionIndex,
```

Please notice that one `HashBuilderOperator` is mapped to one build side partiton, which means that the `partitionIndex` is assigned to this `HashBuilderOperator`.

Then how does the `lookupSourceFactory` contructed? 

```java
public HashBuilderOperator createOperator(DriverContext driverContext)
{
    checkState(!closed, "Factory is already closed");
    OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());

    PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(driverContext.getLifespan());
```

From above code, we could see that the `JoinBridgeManager` will return a `PartitionedLookupSourceFactory` according to the `LifeSpan`.

For the `LookupJoinOperator`, it also has a `lookupSourceFactory` and it is also returned by the `JoinBridgeManager` with the `LifeSpan`:

```java
@Override
public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
{
    LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(processorContext.getLifespan());

    joinBridgeManager.probeOperatorCreated(processorContext.getLifespan());
    return new LookupJoinOperator(
            probeTypes,
            buildOutputTypes,
            joinType,
            lookupSourceFactory,
```



Let's check the details of the `PartitionedLookupSourceFactory`:

It contains the spillingInfo which is in fact the current spilling mask information;

```java
public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
{
    lock.writeLock().lock();
    try {
        checkState(!destroyed.isDone(), "already destroyed");
        if (lookupSourceSupplier != null) {
            //
            return immediateFuture(new SpillAwareLookupSourceProvider());
        }

        SettableFuture<LookupSourceProvider> lookupSourceFuture = SettableFuture.create();
        lookupSourceFutures.add(lookupSourceFuture);
        return lookupSourceFuture;
    }
    finally {
        lock.writeLock().unlock();
    }
}
```

By calling `PartitionedLookupSourceFactory.createLookupSourceProvider()`, the `LookupJoinOperator` will get a provider which could provides the spilling information:

Let's check the details of `SpillAwareLookupSourceProvider`:

```java
@NotThreadSafe
private class SpillAwareLookupSourceProvider
        implements LookupSourceProvider
{
    @Override
    public <R> R withLease(Function<LookupSourceLease, R> action)
    {
        lock.readLock().lock();
        try {
            LookupSource lookupSource = suppliedLookupSources.computeIfAbsent(this, k -> lookupSourceSupplier.getLookupSource());
            LookupSourceLease lease = new SpillAwareLookupSourceLease(lookupSource, spillingInfo);
            return action.apply(lease);
        }
        finally {
            lock.readLock().unlock();
        }
    }
```



From my understanding, `LookupSource` is used to decide the current join status and return the join position when the probe side data arrived. If the build side table is partitioned, then the implements of `LookupSource`  will be `PartitionedLookupSource`.

`LookupSourceLease` is an wrapper for `LookupSource` with additional information about the spilling:

```java
interface LookupSourceLease
{
    LookupSource getLookupSource();

    boolean hasSpilled();

    long spillEpoch();

    IntPredicate getSpillMask();
}
```

We could see that the `SpillAwareLookupSourceLease` is constructed with the `lookupSource` and the `spillingInfo`:

```java
public SpillAwareLookupSourceLease(LookupSource lookupSource, SpillingInfo spillingInfo)
{
    this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
    this.spillingInfo = requireNonNull(spillingInfo, "spillingInfo is null");
}
```

In the `withLease()` method, it will try to apply the lease on the action. It is called in the `LookupJoinOperator.JoinProcessor.processProbe()`

```java
private void processProbe()
{
    verifyNotNull(probe);
    
    Optional<SpillInfoSnapshot> spillInfoSnapshotIfSpillChanged = lookupSourceProvider.withLease(lookupSourceLease -> {
        // From the code of setPartitionSpilledLookupSourceHandle(), we could see that the spill epoch 
        // will increment setPartitionSpilledLookupSourceHandle in which new partition is decided to spill
        if (lookupSourceLease.spillEpoch() == inputPageSpillEpoch) {
            // Spill state didn't change, so process as usual.
            processProbe(lookupSourceLease.getLookupSource());
            return Optional.empty(); // return null when the spill epoch is not changed
        }

        return Optional.of(SpillInfoSnapshot.from(lookupSourceLease));
    });

    if (!spillInfoSnapshotIfSpillChanged.isPresent()) {
        return;
    }
    SpillInfoSnapshot spillInfoSnapshot = spillInfoSnapshotIfSpillChanged.get();
    long joinPositionWithinPartition;
    if (joinPosition >= 0) {
        joinPositionWithinPartition = lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().joinPositionWithinPartition(joinPosition));
    }
    else {
        joinPositionWithinPartition = -1;
    }
```

From above `processProbe()`, it will try to get the` LookupSource` from the `LookupSourceLease` and `lookupSourceLease` is maintained by `LookupSourceProvider`. The `lookupSourceProvider` be created by `PartitionedLookupSourceFactory`



### Try to Unspill the LookupSource

Below is the stacktrace that the `LookupJoin` operator will try to unspill the lookup source and do the look up join.

<img src="https://raw.githubusercontent.com/VicoWu/leetcode/3d5686b1f96d892bb5c6ac46092b4a1a39ae7795/src/main/resources/images/presto/unspill_lookup_source_in_look_up_join_operator.png" alt="canRunMore" width="1000" />



Below is the code of `JoinProcessor.getOutput()`:

```java
private Page getOutput()
{
    // ....skip
    if (probe == null && pageBuilder.isEmpty() && !finishing) {
        return null;
    }

    // There is not probes already, then finish this operator(Please rememeber that Join & Aggregationa are all concurrent operatos)
    if (probe == null && finishing && !unspilling) {
        /*
         * We do not have input probe and we won't have any, as we're finishing.
         * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
         */
        verify(partitionedConsumption == null, "partitioned consumption already started");
        // PartitionedLookupSourceFactory.finishProbeOperator()
        partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
        unspilling = true;
    }

    if (probe == null && unspilling && !finished) {
        /*
         * If no current partition or it was exhausted, unspill next one.
         * Add input there when it needs one, produce output. Be Happy.
         */
        tryUnspillNext(); // Unspill the lookupsource and input pages
    }

    if (probe != null) {
        processProbe();
    }

    if (outputPage != null) {
        verify(pageBuilder.isEmpty());
        Page output = outputPage;
        outputPage = null;
        return output;
    }

    // It is impossible to have probe == null && !pageBuilder.isEmpty(),
    // because we will flush a page whenever we reach the probe end
    verify(probe != null || pageBuilder.isEmpty());
    return null;
}
```

We should notice that:

1. The unspill only happen when the probe side finds that there is no probe data anymore and 
2. The unspill will fetch a partition and then unspill a probe data for this partition. 

In the method of `getOutput()`, it will try to check whether the probe data is null. If it is null, maybe the probe data has been spilled. Then, we need to unspill the probe data. Only probe is unspilled and not null, then we will try to `processProbe()`



```java
private void tryUnspillNext()
{
    verify(probe == null);

    // partitionedConsumption is not done yet, we conitinue
    // partitionedConsumption is intermediate future, so isDone() always return true
    if (!partitionedConsumption.isDone()) {
        return;
    }

    // Get the spilled partition index iterator
    if (lookupPartitions == null) {
        lookupPartitions = getDone(partitionedConsumption).beginConsumption();
    }

    // If we have more unspilledInputPages,we will directly use the unspilledInputPages
    if (unspilledInputPages.hasNext()) {
        addInput(unspilledInputPages.next());
        return;
    }

    // The unspilledInputPages didn't have next, we need to unspill the input pages(probe side)

    // We have unspilled lookup source, it proves that this method tryUnspillNext() has been called previously
    // And the lookupSource has been unspilled already.
    if (unspilledLookupSource.isPresent()) {
        // unspilled lookup source is present, but it is not ready yet
        if (!unspilledLookupSource.get().isDone()) {
            // Not unspilled yet
            return;
        }
        // unspilled lookup source is ready
        // Get the unspilled lookupsource, implements is PartitionedLookupSource
        LookupSource lookupSource = getDone(unspilledLookupSource.get()).get();
        unspilledLookupSource = Optional.empty();

        // Close previous lookupSourceProvider (either supplied initially or for the previous partition)
        lookupSourceProvider.close();
        lookupSourceProvider = new StaticLookupSourceProvider(lookupSource);
        // If the partition was spilled during processing, its position count will be considered twice.
        statisticsCounter.updateLookupSourcePositions(lookupSource.getJoinPositionCount());

        // Get the partiton index
        int partition = currentPartition.get().number();
        // We will unspill the pages for current partition. This spiller is the probe side spiller
        // So pages in the unspilledInputPages should belongs to one single partition
        unspilledInputPages = spiller.map(spiller -> spiller.getSpilledPages(partition))
                .orElse(emptyIterator());

        // Restore some probe's metedata information like join position
        // savedRows.remove(partition) will remove the partition from it and get the corresponding SavedRow
        // Since this row has been unspilled, remove it from the saveRows
        Optional.ofNullable(savedRows.remove(partition)).ifPresent(savedRow -> {
            restoreProbe(
                    savedRow.row,
                    savedRow.joinPositionWithinPartition,
                    savedRow.currentProbePositionProducedRow,
                    savedRow.joinSourcePositions,
                    SpillInfoSnapshot.noSpill());
        });

        return;
    }
    // unspilled lookup source is not present

    if (lookupPartitions.hasNext()) { // try to fetch the next partition that we will try to unspill
        currentPartition.ifPresent(Partition::release);
        // setup the currentPartition. Then the currentPartition will be used to unspill the probe side data
        // in the next time tryUnspillNext() is called.
        currentPartition = Optional.of(lookupPartitions.next());
        // currentPartition是一个可以load 对应的lookupsource的supplier，由于我们需要对这个probe进行处理，因此，对应的LookupSource也需要load进来
        unspilledLookupSource = Optional.of(currentPartition.get().load());

        return;
    }
    .....
    spiller.ifPresent(PartitioningSpiller::verifyAllPartitionsRead);
    finished = true;
}
```



`tryUnspillNext()` will be called only when we find that the probe is null and thus we need to get the next spill data by unspilling from disk.

`PartitionedConsumption` is constructed in the `finishProbeOperator()`:

`PartitionedConsumption` is a wrapper for currently still spilled partition and its loading operation, it will help us to return a `Partition` Iterator named `lookupPartition`, and then, by this Iterator, we could fetch(consume) a partition and then,load the partiton lookupSource.

```java
if (lookupPartitions.hasNext()) { // try to fetch the next partition that we will try to unspill
    currentPartition.ifPresent(Partition::release);
    currentPartition = Optional.of(lookupPartitions.next()); // fetch a partition which will be unspilled in this Interator
    // Then upspill the lookupSource of this partition
    unspilledLookupSource = Optional.of(currentPartition.get().load());
    return;
}
```

Since the `unspilledLookupSource` is now setup and the `currentPartition` is set up, so, the next time `tryUnspillNext()` is called and below code will be executed:

```    java
// unspilled lookup source is present, but it is not ready yet
        if (!unspilledLookupSource.get().isDone()) {
            // Not unspilled yet
            return;
        }
        // unspilled lookup source is ready
        // Get the unspilled lookupsource, implements is PartitionedLookupSource
        LookupSource lookupSource = getDone(unspilledLookupSource.get()).get();
        unspilledLookupSource = Optional.empty();

        // Close previous lookupSourceProvider (either supplied initially or for the previous partition)
        lookupSourceProvider.close();
        lookupSourceProvider = new StaticLookupSourceProvider(lookupSource);
        // If the partition was spilled during processing, its position count will be considered twice.
        statisticsCounter.updateLookupSourcePositions(lookupSource.getJoinPositionCount());

        // Get the partiton index
        int partition = currentPartition.get().number();
        // We will unspill the pages for current partition. This spiller is the probe side spiller
        // So pages in the unspilledInputPages should belongs to one single partition
        unspilledInputPages = spiller.map(spiller -> spiller.getSpilledPages(partition))
                .orElse(emptyIterator());

        // Restore some probe's metedata information like join position
        // savedRows.remove(partition) will remove the partition from it and get the corresponding SavedRow
        // Since this row has been unspilled, remove it from the saveRows
        Optional.ofNullable(savedRows.remove(partition)).ifPresent(savedRow -> {
            restoreProbe(
                    savedRow.row,
                    savedRow.joinPositionWithinPartition,
                    savedRow.currentProbePositionProducedRow,
                    savedRow.joinSourcePositions,
                    SpillInfoSnapshot.noSpill());
        });

        return;
```

The jobs obove code snippet is used to unspill the probe side data of specified partiton(The partiton number is already decided by prevous call of `unspillNext()` , then set up and restore the join information.





```java
@Override
public ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> finishProbeOperator(OptionalInt lookupJoinsCount)
{
    lock.writeLock().lock();
    try {
        if (!spillingInfo.hasSpilled()) { // We didn't have any spill probes
            finishedProbeOperators++;
            return immediateFuture(new PartitionedConsumption<>(
                    1,
                    emptyList(),
                    i -> {
                        throw new UnsupportedOperationException();
                    },
                    i -> {}));
        }

        // We still have some spilled operators
        int operatorsCount = lookupJoinsCount
                .orElseThrow(() -> new IllegalStateException("A fixed distribution is required for JOIN when spilling is enabled"));
        checkState(finishedProbeOperators < operatorsCount, "%s probe operators finished out of %s declared", finishedProbeOperators + 1, operatorsCount);

        if (!partitionedConsumptionParticipants.isPresent()) {
            // This is the first probe to finish after anything has been spilled.
            partitionedConsumptionParticipants = OptionalInt.of(operatorsCount - finishedProbeOperators);
        }

        finishedProbeOperators++;
        // the probe operators has finished
        if (finishedProbeOperators == operatorsCount) {
            // We can dispose partitions now since as right outer is not supported with spill
            freePartitions();
            verify(!partitionedConsumption.isDone());
            partitionedConsumption.set(new PartitionedConsumption<>(
                    partitionedConsumptionParticipants.getAsInt(),
                    spilledPartitions.keySet(),
                    this::loadSpilledLookupSource,
                    this::disposeSpilledLookupSource));
        }

        return partitionedConsumption;
    }
    finally {
        lock.writeLock().unlock();
    }
}
```



We could see that for the probe side spilling and unspilling, the local memory context is not changed or cared because the probe side data is always streamedly ingested and streamedly unspilled so the memory consumption is insignificant. But for the build side spilling and unspilling, the local memory context is setup accordingly.





### HashGenerator for Probe

```java
requireNonNull(probeHashChannel, "probeHashChannel is null");
if (probeHashChannel.isPresent()) {
    this.probeHashGenerator = new PrecomputedHashGenerator(probeHashChannel.getAsInt());
}
else {
    requireNonNull(probeJoinChannels, "probeJoinChannels is null");
    List<Type> hashTypes = probeJoinChannels.stream()
            .map(probeTypes::get)
            .collect(toImmutableList());
    this.probeHashGenerator = new InterpretedHashGenerator(hashTypes, probeJoinChannels);
}
```





## Spilling in Window

`visitWindow()`


## Spilling in Sort

`visitSort()`



## Spilling In Aggregation 
`createHashAggregationOperatorFactory()`



## **SingleStreamSpillerFactory**



## **PartitioningSpillerFactory**

Interface of `PartitioningSpiller`

```java
public interface PartitioningSpiller
        extends Closeable
{
    /**
     * Partition page and enqueue partitioned pages to spill writers.
     * {@link PartitioningSpillResult#getSpillingFuture} is completed when spilling is finished.
     * <p>
     * This method may not be called if previously initiated spilling is not finished yet.
     */
    PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask);

    /**
     * Returns iterator of previously spilled pages from given partition. Callers are expected to call
     * this method once. Calling multiple times can results in undefined behavior.
     * <p>
     * This method may not be called if previously initiated spilling is not finished yet.
     * <p>
     * This method may perform blocking I/O to flush internal buffers.
     */
    // TODO getSpilledPages should not need flush last buffer to disk
    Iterator<Page> getSpilledPages(int partition);

    void verifyAllPartitionsRead();

    /**
     * Closes and removes all underlying resources used during spilling.
     */
    @Override
    void close()
            throws IOException;

    class PartitioningSpillResult
    {
        private final ListenableFuture<?> spillingFuture;
        private final Page retained;

        public PartitioningSpillResult(ListenableFuture<?> spillingFuture, Page retained)
        {
            this.spillingFuture = requireNonNull(spillingFuture, "spillingFuture is null");
            this.retained = requireNonNull(retained, "retained is null");
        }

        public ListenableFuture<?> getSpillingFuture()
        {
            return spillingFuture;
        }

        public Page getRetained()
        {
            return retained;
        }
    }
}
```



This is the pr which is used to add spill for join: https://github.com/prestodb/presto/commit/95ad82b47530f8ecef59bd1afc215894965e2acd



```java
private OperatorFactory createLookupJoin(
        JoinNode node,
        ......
        boolean spillEnabled)
{
    switch (node.getType()) {
        case INNER:
            return lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
        case LEFT:
            return lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
        case RIGHT:
            return lookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
        case FULL:
            return lookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
        default:
            throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
    }
}
```



In `LookupJoinOperator`, 

```java
private Page spillAndMaskSpilledPositions(Page page, IntPredicate spillMask)
{
    checkState(spillInProgress.isDone(), "Previous spill still in progress");
    checkSuccess(spillInProgress, "spilling failed");

    if (!spiller.isPresent()) {
        spiller = Optional.of(partitioningSpillerFactory.create(
                probeTypes,
                getPartitionGenerator(),
                spillContext.newLocalSpillContext(),
                memoryTrackingContext.newAggregateSystemMemoryContext()));
    }

    PartitioningSpillResult result = spiller.get().partitionAndSpill(page, spillMask);
    spillInProgress = result.getSpillingFuture();
    return result.getRetained();
}
```





# References

[What is Index Join](https://prestodb.rocks/internals/the-fundamentals-join-algorithms)

[Dynamic filtering for highly selective join optimization](https://prestosql.io/blog/2019/06/30/dynamic-filtering.html)

[Presto Sql Join Operator](https://www.javahelps.com/2019/11/presto-sql-join-algorithms.html)


