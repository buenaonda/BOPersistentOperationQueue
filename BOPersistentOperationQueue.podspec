Pod::Spec.new do |s|
    s.name = "BOPersistentOperationQueue"
    s.version = "0.1"
    s.summary = "A NSOperationQueue that will persist tasks beyond one runtime. A Resque for Cocoa."
    s.license = "Evil Private License"
    s.authors  = { 'Diego Torres' => 'contact@dtorres.me'}
    s.homepage = "http://www.dtorres.me"
    s.source_files = "**/BOPersistentOperationQueue.{h,m}", "**/NSOperation+PersistanceID.{h,m}", "**/BOOperationPersistance.h"
    s.source   = { :git => 'gitolite@66.175.220.174:BOPersistentOperationQueue', :tag => s.version.to_s }
    s.library = 'sqlite3.0'
    s.requires_arc = true
    s.dependency 'FMDB'
end
