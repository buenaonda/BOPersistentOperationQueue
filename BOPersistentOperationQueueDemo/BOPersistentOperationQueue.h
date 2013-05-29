//
//  BOPersistentOperationQueue.h
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/8/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "BOOperationPersistance.h"

extern NSString * const BOPersistentOperationIdentifier;

@interface BOPersistentOperationQueue : NSOperationQueue

- (void)removeOperation:(NSOperation <BOOperationPersistance> *)op;
- (void)removeOperationWithIdentifier:(NSNumber *)identifier;

/** 
 WARNING: POTENTIALLY EXPENSIVE OPERATION.
          USE CAREFULLY.
*/
- (NSArray *)pendingDataOfOperationsWithClass:(Class <BOOperationPersistance>)operationClass;
- (NSArray *)pendingDataOfOperationsWithClass:(Class <BOOperationPersistance>)operationClass like:(NSString *)query;

@end
