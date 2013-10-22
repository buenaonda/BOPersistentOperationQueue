//
//  BOOperationPersistance.h
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/8/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import <Foundation/Foundation.h>

static NSString *BOOperationDidInvalidateOperationData = @"BOOperationDidInvalidateOperationData";

@protocol BOOperationPersistance <NSObject>

+ (instancetype)operationWithDictionary:(NSDictionary *)operationData;
- (NSDictionary *)operationData;
- (BOOL)finishedSuccessfully;

@optional
/** 
 shouldPersist defaults to YES if this is not implemented.
*/
- (BOOL)shouldPersist;
+ (void)willRemoveOperationWithDictionary:(NSDictionary *)operationData;
- (void)remove;

@end
