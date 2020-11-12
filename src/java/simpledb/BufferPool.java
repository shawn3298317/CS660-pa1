package simpledb;

import sun.security.ssl.Record;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int _numPages;

    private HashMap<PageId, Page> _pagePool; // cache w/t limited memory (DEFAULT_PAGES).
    private LinkedList<PageId> _pageRecencyList; // doubly linked list that keep tracks of page recency.

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this._numPages = numPages;
        this._pagePool = new HashMap<>();
        this._pageRecencyList = new LinkedList<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (_pagePool.containsKey(pid)) {
            // update recency accordingly
            updateRecency(pid);
            return _pagePool.get(pid);
        } else {
            // retrieve page data
            int tableId = pid.getTableId();
            // HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
            DbFile df = Database.getCatalog().getDatabaseFile(tableId);

            // detect if _pagePool is full, evict if fulled.
            if (_pagePool.size() >= _numPages) {
                Debug.log("BufferPool reached maximum pages (%d), evicting page now...", _numPages);
                evictPage();
            }
            // HeapPage page = (HeapPage) hf.readPage(pid);
            // _pagePool.put(pid, new HeapPage((HeapPageId) pid, page.getPageData()));
            Page page = df.readPage(pid);

            _pagePool.put(pid, page);
            updateRecency(pid);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // TODO: implement locks in PA3

        // find dirtied pages
        DbFile df = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtied_pages = df.insertTuple(tid, t);

        // mark dirty bits for all the dirtied pages
        for (Page p: dirtied_pages) {
            p.markDirty(true, tid);
            PageId pid = p.getId();
            if (!_pagePool.containsKey(p.getId())) {
                if (_pagePool.size() > pageSize)
                    evictPage();
            }
            _pagePool.put(p.getId(), p);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // find the page that contains the tuple in _pagePool
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
        ArrayList<Page> dirtied_pages = df.deleteTuple(tid, t);

        for (Page p: dirtied_pages) {
            p.markDirty(true, tid);

            if (!_pagePool.containsKey(p.getId())) {
                if (_pagePool.size() > pageSize)
                    evictPage();
            }
            _pagePool.put(p.getId(), p);

        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        // TODO: implement this...

//        Iterator it = _pagePool.entrySet().iterator();
//
//        if (it.hasNext()) {
//            Map.Entry kv = (Map.Entry) it.next();
//            Page p = (Page) kv.getValue();
//            PageId pid = (PageId) kv.getKey();
//            if (p.isDirty()) {
//
//            }
//        }
        System.out.println("Not implemented here flushAllPages()!!!");
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        int tableId = pid.getTableId();
        DbFile df = Database.getCatalog().getDatabaseFile(tableId);
        Page removedPage = _pagePool.remove(pid);
        df.writePage(removedPage);
        removedPage.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1r

        // determine which page to evict: LRU policy
        PageId flush_pid = _pageRecencyList.removeLast();
        // MRU policy
        // PageId flush_pid = _pageRecencyList.removeFirst();

        // flush popped page to disk
        try {
            flushPage(flush_pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Find pid in recencyList and move it to list head.
     */
    private synchronized void updateRecency(PageId pid) {
        int node_index = _pageRecencyList.indexOf(pid); // O(N)
        if (node_index >= 0 && node_index < _pageRecencyList.size())
            _pageRecencyList.remove(node_index);
        _pageRecencyList.addFirst(pid);
    }

}
