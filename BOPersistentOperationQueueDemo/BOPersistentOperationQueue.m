//
//  BOPersistentOperationQueue.m
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/8/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import "BOPersistentOperationQueue.h"
#import "NSOperation+Persistance.h"
#import <FMDB/FMDatabase.h>
#import <FMDB/FMDatabaseQueue.h>
#import <FMDB/FMDatabaseAdditions.h>

static NSString * const defaultQueueDomainName = @"com.buenaonda.BOPersistentOperationQueue";
NSString * const BOPersistentOperationIdentifier = @"BOPersistentOperationIdentifier";
NSString * const BOPersistentOperationClass = @"BOPersistentOperationClass";

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
        if (op.pendingRetryAttempts == nil) {
            op.pendingRetryAttempts = @(-1);
        }
        if (op.identifier) {
            //Already in DB, bye!
            return;
        }
        //Register in DB
        NSDictionary *operationDictionary = [op operationData];
        NSError *error;
        NSData *operationData = nil;
        if (operationDictionary) {
            operationData = [NSJSONSerialization dataWithJSONObject:operationDictionary options:0 error:&error];
        }
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
            [database executeUpdate:@"ALTER TABLE `jobs` ADD `operationRetry` INT  NULL  DEFAULT NULL AFTER `operationData`"];
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
                NSString *query = [NSString stringWithFormat:@"SELECT * FROM `jobs` WHERE id > '%ld' AND id < '%ld' LIMIT 0,10", (long int)_lastRetrievedId, (long int)_smallestIdCreatedOnRuntime];
                FMResultSet *result = [database executeQuery:query];
                while ([result next]) {
                    Class<BOOperationPersistance> operationClass = NSClassFromString([result stringForColumnIndex:1]);
                    NSData *operationData = [result dataForColumnIndex:2];
                    NSError *error;
                    NSDictionary *operationDictionary = operationData != nil ? [NSJSONSerialization JSONObjectWithData:operationData options:0 error:&error] : nil;
                    NSUInteger identifier = [result intForColumnIndex:0];
                    NSInteger retry = [result intForColumnIndex:3];
                    _lastRetrievedId = identifier;
                    
                    NSOperation <BOOperationPersistance> *op = [operationClass operationWithDictionary:operationDictionary];
                    if (op) {
                        op.identifier = [NSNumber numberWithUnsignedInteger:identifier];
                        op.pendingRetryAttempts = @(retry);
                        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0), ^{
                            [self addOperation:op];
                        });
                    }
                }
            }];
        });
    }
}

- (NSArray *)pendingDataOfOperationsWithClass:(Class<BOOperationPersistance>)operationClass
{
    return [self pendingDataOfOperationsWithClass:operationClass like:nil];
}

- (NSArray *)pendingDataOfOperationsWithClass:(Class<BOOperationPersistance>)operationClass like:(NSString *)likeQuery
{
    NSMutableArray *pendingData = [NSMutableArray new];
    if (!likeQuery) {
        likeQuery = @"%";
    } else {
        likeQuery = [NSString stringWithFormat:@"%%%@%%", likeQuery];
    }
    NSString *classString = operationClass ? NSStringFromClass(operationClass) : @"%";
    [_dbQueue inDatabase:^(FMDatabase *database) {
        NSString *query = [NSString stringWithFormat:@"SELECT * FROM `jobs` WHERE operationClass LIKE '%@' AND operationData LIKE '%@'", classString, likeQuery];

        FMResultSet *result = [database executeQuery:query];
        while ([result next]) {
            NSData *operationData = [result dataForColumnIndex:2];
            NSError *error;
            NSMutableDictionary *operationDictionary = [[NSJSONSerialization JSONObjectWithData:operationData options:0 error:&error] mutableCopy];
            [operationDictionary setObject:[result objectForColumnIndex:0]
                                    forKey:BOPersistentOperationIdentifier];
            [operationDictionary setObject:[result objectForColumnIndex:1]
                                    forKey:BOPersistentOperationClass];
            [pendingData addObject:operationDictionary];
        }
    }];
    return pendingData;
}

- (void)removeFromDatabaseJobWithIdentifier:(NSNumber *)identifier
{
    [_dbQueue inDatabase:^(FMDatabase *db) {
        NSString *sql = [NSString stringWithFormat:@"DELETE FROM `jobs` WHERE id = '%@'", identifier];
        [db executeUpdate:sql];
    }];
}

- (void)removeOperation:(NSOperation<BOOperationPersistance> *)op
{
    if ([op respondsToSelector:@selector(remove)]) {
        [op remove];
    }
    [self removeFromDatabaseJobWithIdentifier:op.identifier];
}

- (void)removeOperationWithIdentifier:(NSNumber *)identifier
{
    NSPredicate *predicate = [NSPredicate predicateWithFormat:@"identifier = %@", identifier];
    NSOperation <BOOperationPersistance> *op = [[self.operations filteredArrayUsingPredicate:predicate] lastObject];
    if (op) {
        return [self removeOperation:op];
    }
    [self removeFromDatabaseJobWithIdentifier:identifier];
}

- (void)removeAllPendingOperations
{
    [self.operations enumerateObjectsWithOptions:NSEnumerationConcurrent usingBlock:^(NSOperation <BOOperationPersistance> * obj, NSUInteger idx, BOOL *stop) {
        [self removeOperation:obj];
    }];
    
    NSArray *pendingOpsData = [self pendingDataOfOperationsWithClass:Nil];
    [pendingOpsData enumerateObjectsWithOptions:NSEnumerationConcurrent usingBlock:^(NSDictionary *obj, NSUInteger idx, BOOL *stop) {
        Class <BOOperationPersistance> operationClass = NSClassFromString([obj objectForKey:BOPersistentOperationClass]);
        if ([(id)operationClass respondsToSelector:@selector(willRemoveOperationWithDictionary:)]) {
            [operationClass willRemoveOperationWithDictionary:obj];
        }
    }];
    [_dbQueue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"TRUNCATE `jobs`"];
    }];
}

- (void)decreaseRetry:(NSOperation <BOOperationPersistance> *)op
{
    if (op.pendingRetryAttempts != nil && op.pendingRetryAttempts.intValue > 0) {
        op.pendingRetryAttempts = @(op.pendingRetryAttempts.intValue - 1);
    }
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        NSString *query = [NSString stringWithFormat:@"UPDATE `jobs` SET operationRetry = %@ WHERE id = '%@'", op.pendingRetryAttempts, op.identifier];
        [db executeUpdate:query];
    }];
}

#pragma mark - KVO

- (void)observeValueForKeyPath:(NSString *)keyPath ofObject:(id)object change:(NSDictionary *)change context:(void *)context
{
    if ([object isKindOfClass:[NSOperation class]]) {
        NSOperation<BOOperationPersistance> *operation = object;
        if ([keyPath isEqualToString:@"isFinished"]) {
            NSLog(@"(%@) Finished, Retry = %@", operation.identifier, operation.pendingRetryAttempts);
            BOOL success = [operation finishedSuccessfully];
            if (success || [operation.pendingRetryAttempts isEqualToNumber:@(0)]) {
                if (!success) {
                    [self removeOperation:operation];
                } else {
                    [self removeFromDatabaseJobWithIdentifier:operation.identifier];
                }
            } else {
                [self decreaseRetry:operation];
                NSOperation <BOOperationPersistance> *op = [[operation class] operationWithDictionary:[operation operationData]];
                if (op) {
                    op.identifier = operation.identifier;
                    op.pendingRetryAttempts = operation.pendingRetryAttempts;
                    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0), ^{
                        [self addOperation:op];
                    });
                }
            }
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

- (void)cancelAllOperations
{
    BOOL wasSuspended = [self isSuspended];
    [self setSuspended:YES];
    [self removeAllPendingOperations];
    [super cancelAllOperations];
    [self setSuspended:wasSuspended];
}

- (NSUInteger)operationCount
{
    NSUInteger operationCount = [super operationCount];
    __block NSUInteger countInDB = 0;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *result = [db executeQuery:@"SELECT count(*) FROM `jobs`"];
        while ([result next]) {
            countInDB = [result intForColumnIndex:0];
        }
    }];
    return MAX(operationCount, countInDB);
}

- (void)dealloc
{
    [[NSNotificationCenter defaultCenter] removeObserver:self];
}

@end
