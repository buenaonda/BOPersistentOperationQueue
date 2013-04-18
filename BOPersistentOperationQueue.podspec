Pod::Spec.new do |s|
    s.name = "BOPersistentOperationQueue"
    s.version = "0.0.1"
    s.summary = "A NSOperationQueue that will persist tasks beyond one runtime. A Resque for Cocoa."
    s.license = "Evil Private License"
    s.source_files = "**/BOPersistentOperationQueue.{h,m}", "**/NSOperation+PersistanceID.{h,m}", "**/BOOperationPersistance.h"
    s.library = 'sqlite3.0'
    s.requires_arc = true
    s.dependency 'FMDB'
end
