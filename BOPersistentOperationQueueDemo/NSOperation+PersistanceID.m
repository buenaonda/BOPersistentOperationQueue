//
//  NSOperation+PersistanceID.m
//  BOPersistentOperationQueueDemo
//
//  Created by Diego Torres on 4/12/13.
//  Copyright (c) 2013 Buena Onda. All rights reserved.
//

#import "NSOperation+PersistanceID.h"
#import <objc/runtime.h>

static char const * const PersistanceIDKey = "PersistanceID";

@implementation NSOperation (PersistanceID)
@dynamic identifier;

- (NSNumber *)identifier
{
    return objc_getAssociatedObject(self, PersistanceIDKey);
}

- (void)setIdentifier:(NSNumber *)identifier
{
    objc_setAssociatedObject(self, PersistanceIDKey, identifier, OBJC_ASSOCIATION_RETAIN_NONATOMIC);
}

@end
