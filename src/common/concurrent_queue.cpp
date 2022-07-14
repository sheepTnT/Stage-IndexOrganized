

//#include "../include/common/logger.h"
#include "../include/common/concurrent_queue.h"
//#include "../include/vstore/version_store.h"
//#include "../include/common/constants.h"

namespace mvstore {


// Explicit template instantiation
template class ConcurrentQueue<void *>;

// Used in Catalog::Manager
//template class ConcurrentQueue<VersionBlock *>;

// Used in Shared Pointer test and iterator test
//template class ConcurrentQueue<VersionBlockElem>;


}
