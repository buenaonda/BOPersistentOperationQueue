//
//  BOPersistentOperationQueue.h
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/8/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "BOOperationPersistance.h"

@interface BOPersistentOperationQueue : NSOperationQueue

/** 
 WARNING: POTENTIALLY EXPENSIVE OPERATION.
          USE CAREFULLY.
*/
- (NSArray *)pendingDataOfOperationsWithClass:(Class <BOOperationPersistance>)operationClass;

@end
