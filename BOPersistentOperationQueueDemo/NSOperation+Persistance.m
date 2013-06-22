//
//  NSOperation+Persistance.m
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/12/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import "NSOperation+Persistance.h"
#import <objc/runtime.h>

static char const * const PersistanceIDKey = "PersistanceID";
static char const * const PersistanceRetryKey = "RetryAttemptsLeft";

@implementation NSOperation (Persistance)
@dynamic identifier, pendingRetryAttempts;

- (NSNumber *)identifier
{
    return objc_getAssociatedObject(self, PersistanceIDKey);
}

- (void)setIdentifier:(NSNumber *)identifier
{
    objc_setAssociatedObject(self, PersistanceIDKey, identifier, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
}

- (NSNumber *)pendingRetryAttempts
{
    return objc_getAssociatedObject(self, PersistanceRetryKey);
}

- (void)setPendingRetryAttempts:(NSNumber *)pendingRetryAttempts
{
    objc_setAssociatedObject(self, PersistanceRetryKey, pendingRetryAttempts, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
}

@end
