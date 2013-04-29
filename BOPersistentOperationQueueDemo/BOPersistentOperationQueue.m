//
//  BOPersistentOperationQueue.m
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/8/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import "BOPersistentOperationQueue.h"
#import "NSOperation+PersistanceID.h"
#import <FMDB/FMDatabase.h>
#import <FMDB/FMDatabaseQueue.h>
#import <FMDB/FMDatabaseAdditions.h>

static NSString * const defaultQueueDomainName = @"com.buenaonda.BOPersistentOperationQueue";

@interface BOPersistentOperationQueue () {
    BOOL _started;
    NSInteger _smallestIdCreatedOnRuntime;
    NSInteger _lastRetrievedId;
    dispatch_queue_t _jobRetrievalQueue;
    BOOL _retrievingJobs;
}

@property (nonatomic, strong) FMDatabaseQueue *dbQueue;

@end

@implementation BOPersistentOperationQueue

- (id)init
{
    self = [super init];
    if (self) {
        _started = NO;
        _smallestIdCreatedOnRuntime = NSIntegerMax;
        _lastRetrievedId = 0;
        _retrievingJobs = NO;
        _jobRetrievalQueue = dispatch_queue_create("com.buenaonda.BOPersistentOperationQueue.jobRetrieval", DISPATCH_QUEUE_SERIAL);
        [self addObserver:self forKeyPath:@"operations" options:0 context:NULL];
    }
    return self;
}

#pragma mark - Class methods

- (void)setupOperationPersistence:(NSOperation <BOOperationPersistance> *)op
{
    BOOL shouldPersist = [op respondsToSelector:@selector(shouldPersist)] ? [op shouldPersist] : YES;

    if([op conformsToProtocol:@protocol(BOOperationPersistance)] && shouldPersist) {
        [op addObserver:self forKeyPath:@"isFinished" options:0 context:NULL];
        if (op.identifier) {
            //Already in DB, bye!
            return;
        }
        //Register in DB
        NSDictionary *operationDictionary = [op operationData];
        NSError *error;
        NSData *operationData = [NSJSONSerialization dataWithJSONObject:operationDictionary options:0 error:&error];
        NSString *operationString = [[NSString alloc] initWithData:operationData encoding:NSUTF8StringEncoding];
        [_dbQueue inDatabase:^(FMDatabase *db) {
            [db executeUpdate:@"INSERT INTO `jobs` (`operationClass`, `operationData`) VALUES (?, ?)", NSStringFromClass([op class]), operationString];
            NSUInteger lastId = [db lastInsertRowId];
            op.identifier = [NSNumber numberWithInteger:lastId];
            if (_smallestIdCreatedOnRuntime == NSIntegerMax) {
                _smallestIdCreatedOnRuntime = lastId;
            }
        }];
    }
#ifdef DEBUG_BOPERSISTANCE
    else {
        NSLog(@"%@ doesn't conform to BOOperationPersistance protocol. It will run in the queue as normal", op);
    }
#endif
}

- (void)addOperationWithClass:(Class<BOOperationPersistance>)class dictionary:(NSDictionary *)dictionary identifier:(NSNumber *)identifier
{
    NSOperation *operation = [class operationWithDictionary:dictionary];
    if (operation) {
        operation.identifier = identifier;
        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0), ^{
            [self addOperation:operation];
        });
    }
}

- (void)retrievePendingQueue
{
    if (!_dbQueue && self.name != nil) {
        NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask,YES);
        NSString *documentsDirectory = [paths objectAtIndex:0];
        NSString *fileName = [self.name stringByAppendingPathExtension:@"db"];
        NSString *dbPath = [documentsDirectory stringByAppendingPathComponent:fileName];
        _dbQueue = [FMDatabaseQueue databaseQueueWithPath:dbPath];
        [_dbQueue inDatabase:^(FMDatabase *database) {
            [database executeUpdate:@"CREATE TABLE IF NOT EXISTS `jobs` (id INTEGER PRIMARY KEY NOT NULL UNIQUE, operationClass VARCHAR(100) NOT NULL, operationData VARCHAR(500) NULL)"];
            NSUInteger numberOfPendingOps = [database intForQuery:@"SELECT count(id) FROM jobs"];
            if (numberOfPendingOps == 0) {
                _lastRetrievedId = NSIntegerMax;
            }
        }];
    }
    if (_dbQueue && (_lastRetrievedId < (_smallestIdCreatedOnRuntime - 1))) {
        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0), ^{
            [_dbQueue inDatabase:^(FMDatabase *database) {
                //Get task created before this runtime.
                NSString *query = [NSString stringWithFormat:@"SELECT * FROM `jobs` WHERE id > '%d' AND id < '%d' LIMIT 0,10", _lastRetrievedId, _smallestIdCreatedOnRuntime];
                FMResultSet *result = [database executeQuery:query];
                while ([result next]) {
                    Class<BOOperationPersistance> operationClass = NSClassFromString([result stringForColumnIndex:1]);
                    NSData *operationData = [result dataForColumnIndex:2];
                    NSError *error;
                    NSDictionary *operationDictionary = [NSJSONSerialization JSONObjectWithData:operationData options:0 error:&error];
                    NSUInteger identifier = [result intForColumnIndex:0];
                    _lastRetrievedId = identifier;
                    [self addOperationWithClass:operationClass dictionary:operationDictionary identifier:[NSNumber numberWithUnsignedInteger:identifier]];
                }
            }];
        });
    }
}

- (NSArray *)pendingDataOfOperationsWithClass:(Class<BOOperationPersistance>)operationClass
{
    NSMutableArray *pendingData = [NSMutableArray new];
    [_dbQueue inDatabase:^(FMDatabase *database) {
        NSString *query = [NSString stringWithFormat:@"SELECT * FROM `jobs` WHERE operationClass = '%@'", NSStringFromClass(operationClass)];
        FMResultSet *result = [database executeQuery:query];
        while ([result next]) {
            NSData *operationData = [result dataForColumnIndex:2];
            NSError *error;
            NSDictionary *operationDictionary = [NSJSONSerialization JSONObjectWithData:operationData options:0 error:&error];
            [pendingData addObject:operationDictionary];
        }
    }];
    return pendingData;
}

#pragma mark - KVO

- (void)observeValueForKeyPath:(NSString *)keyPath ofObject:(id)object change:(NSDictionary *)change context:(void *)context
{
    if ([object isKindOfClass:[NSOperation class]]) {
        NSOperation<BOOperationPersistance> *operation = object;
        if ([operation finishedSuccessfully]) {
            [_dbQueue inDatabase:^(FMDatabase *db) {
                NSString *sql = [NSString stringWithFormat:@"DELETE FROM `jobs` WHERE id = '%@'", operation.identifier];
                [db executeUpdate:sql];
            }];
        } else {
            [self addOperationWithClass:[operation class] dictionary:[object operationData] identifier:operation.identifier];
        }
    } else if (object == self) {
        if (!_retrievingJobs && (self.operations.count == 0)) {
            _retrievingJobs = YES;
            dispatch_sync(_jobRetrievalQueue, ^{
                [self retrievePendingQueue];
                _retrievingJobs = NO;
            });
        }
    }
}

#pragma mark - Persistency overrides

- (void)setName:(NSString *)name
{
    if (_started) {
        @throw [NSException exceptionWithName:@"NameAlreadySetException" reason:@"The name was already set and/or the queue already started" userInfo:nil];
    }
    [super setName:name];
    _started = YES;
    [self retrievePendingQueue];
}

- (void)addOperation:(NSOperation <BOOperationPersistance> *)op
{
    if (!_started) {
        self.name = defaultQueueDomainName;
    }
    [self setupOperationPersistence:op];
    [super addOperation:op];
}

- (void)addOperations:(NSArray *)ops waitUntilFinished:(BOOL)wait
{
    if (!_started) {
        self.name = defaultQueueDomainName;
    }
    [ops enumerateObjectsWithOptions:(NSEnumerationConcurrent) usingBlock:^(NSOperation <BOOperationPersistance> *op, NSUInteger idx, BOOL *stop) {
        [self setupOperationPersistence:op];
    }];
    [super addOperations:ops waitUntilFinished:wait];
}

- (void)dealloc
{
    [[NSNotificationCenter defaultCenter] removeObserver:self];
}

@end
