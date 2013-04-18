//
//  BOPersistentOperationQueue.m
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/8/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import "BOPersistentOperationQueue.h"
#import "BOOperationPersistance.h"
#import "NSOperation+PersistanceID.h"
#import <FMDB/FMDatabase.h>
#import <FMDB/FMDatabaseQueue.h>

static NSString * const defaultQueueDomainName = @"com.buenaonda.BOPersistentOperationQueue";

@interface BOPersistentOperationQueue () {
    BOOL _started;
}

@property (nonatomic, strong) FMDatabaseQueue *dbQueue;

@end

@implementation BOPersistentOperationQueue

- (id)init
{
    self = [super init];
    if (self) {
        _started = NO;
        NSString *byeNotificationName;
#ifdef __IPHONE_OS_VERSION_MIN_REQUIRED
        byeNotificationName = UIApplicationDidEnterBackgroundNotification;
#else
        byeNotificationName = NSApplicationWillTerminateNotification;
#endif
        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(backupOperations) name:byeNotificationName object:nil];
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
            op.identifier = [NSNumber numberWithInteger:[db lastInsertRowId]];
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
            FMResultSet *result = [database executeQuery:@"SELECT * FROM `jobs`"];
            while ([result next]) {
                Class<BOOperationPersistance> operationClass = NSClassFromString([result stringForColumnIndex:1]);
                NSData *operationData = [result dataForColumnIndex:2];
                NSError *error;
                NSDictionary *operationDictionary = [NSJSONSerialization JSONObjectWithData:operationData options:0 error:&error];
                NSOperation *operation = [operationClass operationWithDictionary:operationDictionary];
                if (operation) {
                    operation.identifier = [result objectForColumnIndex:0];
                    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0), ^{
                        [self addOperation:operation];
                    });
                }
            }
        }];
    }
}

#pragma mark - KVO

- (void)observeValueForKeyPath:(NSString *)keyPath ofObject:(NSOperation<BOOperationPersistance> *)object change:(NSDictionary *)change context:(void *)context
{
    if ([object finishedSuccessfully]) {
        [_dbQueue inDatabase:^(FMDatabase *db) {
            NSString *sql = [NSString stringWithFormat:@"DELETE FROM `jobs` WHERE id = '%@'", object.identifier];
            [db executeUpdate:sql];
        }];
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
