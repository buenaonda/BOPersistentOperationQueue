//
//  NSOperation+PersistanceID.h
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/12/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface NSOperation (PersistanceID)

@property (nonatomic, assign) NSNumber *identifier;

@end
