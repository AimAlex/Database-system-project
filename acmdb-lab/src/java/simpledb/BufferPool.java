package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

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

    private int pageNum;

    private Map<PageId, Page> pageMap;
    private Map<PageId, Integer> pageUsed;
    private LockManager lockManager;
//    private Map<PageId, Object> lockMap;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageNum = numPages;
        pageMap = new ConcurrentHashMap<PageId, Page>();
        pageUsed = new ConcurrentHashMap<PageId, Integer>();
        lockManager = new LockManager();
//        lockMap = new ConcurrentHashMap<PageId, Object>();
        // some code goes here
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
            throws TransactionAbortedException, DbException, InterruptedException {

        // some code goes here
//        if(!lockMap.containsKey(pid)){
//            Object lock = new Object();
//            lockMap.put(pid, lock);
//        }

//        Object lock = lockMap.get(pid);
        //                synchronized(lock) {
        boolean hasLock = lockManager.grantLock(tid, pid, perm);
            while (!hasLock) {
                //System.out.print("grantLock failed\n");

                    if (lockManager.deadLockOccur(tid, pid)) {
//                        synchronized (this) {
//                            //System.out.print(tid);
//                            //System.out.print("***");
//                            //System.out.print(pid);
//                            //System.out.print("\n");
//                        }
                        //System.out.print("deadLock\n");
                        throw new TransactionAbortedException();
                    }
//                synchronized(lock) {
                    hasLock = lockManager.grantLock(tid, pid, perm);
                    Thread.sleep(100);

            }

//                                synchronized (this) {
//                                    //System.out.print("\nlock granted \n");
//                                    //System.out.print(perm);
//                                    //System.out.print("\n");
//                            //System.out.print(tid);
//                            //System.out.print("***");
//                            //System.out.print(pid);
//                            //System.out.print("\n\n");
//                            if(perm.equals(Permissions.READ_WRITE)){
//                                //System.out.print("");
//                            }
//                        }

            addPage(pid);
        return pageMap.get(pid);
    }


    public void addPage(PageId pid) throws DbException {
        if (pageMap.containsKey(pid)) {
            usePage(pid);
            return;
        }
        try{
            while(pageMap.size() >= pageNum){
                evictPage();
            }
            DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page newPage = table.readPage(pid);
//            //System.out.print(((HeapPage)newPage).tuples[1]);
            pageMap.put(pid, newPage);

            usePage(pid);
        } catch (NoSuchElementException e) {
            throw new DbException("");
        }
    }

    public void usePage(PageId pid) {
        if (!pageUsed.containsKey(pid)) {
            pageUsed.put(pid, 0);
        }
        for (Map.Entry<PageId, Integer> entry : pageUsed.entrySet()) {
            if (entry.getKey().equals(pid)) {
                entry.setValue(0);
            } else {
                entry.setValue(entry.getValue() + 1);
            }
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
        // not necessary for lab1|lab2\
            lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
            transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
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

            if (commit) {
                for (Page page : pageMap.values()) {

                    boolean writeLock = lockManager.holdsWriteLock(tid, page.getId());
//                    if (page.isDirty() == null && !writeLock) {
//                        page.setBeforeImage();
//                    }
//                    else if (writeLock || page.isDirty().equals(tid)) {
////                    //System.out.print(tid);
////                    //System.out.print(page.isDirty());
//                        flushPage(page.getId());
////                    //System.out.print(((HeapPage)page).tuples[1]);
////                    //System.out.print(page.isDirty());
//                    }
                    if(writeLock){
                        flushPage((page.getId()));
                    }
                }
            }
            else {
                for (Page page : pageMap.values()) {
//                    if ((page.isDirty() != null && page.isDirty().equals(tid)) || lockManager.holdsWriteLock(tid, page.getId())) {
//
//                        pageMap.put(page.getId(), page.getBeforeImage());
//                        page.markDirty(false, null);
//
//                    }
                    if(lockManager.holdsWriteLock(tid, page.getId())){
                        pageMap.put(page.getId(), page.getBeforeImage());
                        page.markDirty(false, null);
                    }
                }
            }
            lockManager.releaseTidLock(tid);

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
        throws DbException, TransactionAbortedException {


        // some code goes here
        try {
            ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
            for (Page page : pages) {
                getPage(tid, page.getId(), Permissions.READ_WRITE);

                pageMap.put(page.getId(), page);
                page.markDirty(true, tid);
            }
        } catch (DbException| IOException| TransactionAbortedException e){
                e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // not necessary for lab1
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
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException, InterruptedException {
        // some code goes here
        int table = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(table);
            ArrayList<Page> pages = file.deleteTuple(tid, t);
            for (Page page : pages) {
                getPage(tid, page.getId(), Permissions.READ_WRITE);

                pageMap.put(page.getId(), page);
                page.markDirty(true, tid);

            }
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        // some code goes here
        for (PageId page: pageMap.keySet()){
            flushPage(page);
        }
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public void discardPage(PageId pid) {
        // some code goes here
        if (!pageMap.containsKey(pid)) {
            return;
        }
        pageMap.remove(pid);
//        lockMap.remove(pid);
        pageUsed.remove(pid);
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        // some code goes here

        Page page = pageMap.get(pid);
//        //System.out.print(((HeapPage)page).tuples[1]);
//        if(page.isDirty() != null){

        (Database.getCatalog().getDatabaseFile(pid.getTableId())).writePage(page);
        page.markDirty(false, null);
        page.setBeforeImage();
//        }
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page : pageMap.values()) {
            if ((page.isDirty() != null && page.isDirty() == tid)) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        // some code goes here
        PageId pageId = null;

        Integer maximum = 0;
        for (Map.Entry<PageId, Integer> entry : pageUsed.entrySet()) {
            Page tmp = pageMap.get(entry.getKey());
            if(!lockManager.isWriteLock(tmp.getId())){
                if (entry.getValue() >= maximum) {
                    maximum = entry.getValue();
                    pageId = entry.getKey();
                }
            }
        }


        if(pageId == null){
            for (Map.Entry<PageId, Integer> entry : pageUsed.entrySet()) {
                Page tmp = pageMap.get(entry.getKey());
                if(tmp.isDirty() == null) {
                    if (entry.getValue() >= maximum) {
                        maximum = entry.getValue();
                        pageId = entry.getKey();
                    }
                }
            }
        }
        try {
            if(pageId != null) {
                flushPage(pageId);
                discardPage(pageId);
            }
            else{
                throw new DbException("");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // not necessary for lab1
    }

}
