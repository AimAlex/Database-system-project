package simpledb;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LockManager {
    private Map<PageId, TransactionId> writeLocks;
    private Map<PageId, Set<TransactionId>> readLocks;
    private Map<TransactionId, Set<PageId>> readPages;
    private Map<TransactionId, Set<PageId>> writePages;
    private Map<PageId, Set<TransactionId>> grantWrites;
    private Map<PageId, Set<TransactionId>> grantReads;

    public LockManager(){
        writeLocks = new ConcurrentHashMap<PageId, TransactionId>();
        readLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        readPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        writePages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        grantReads = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        grantWrites = new ConcurrentHashMap<PageId, Set<TransactionId>>();
    }

    boolean deadLockOccur(TransactionId tid, PageId pid){
        Set<TransactionId> visit = new HashSet<TransactionId>();

        Queue<TransactionId> queue = new ConcurrentLinkedQueue<>();

        queue.add(tid);

        while(!queue.isEmpty()){
            TransactionId tmp = queue.poll();
            if(visit.contains(tmp)){
                continue;
            }
            visit.add(tmp);
            Set<PageId> readPage = readPages.get(tmp);
            Set<PageId> writePage = writePages.get(tmp);

            if(readPage != null){
                for (PageId page: readPage){
                    Set<TransactionId> grantWrite = grantWrites.get(page);
                    if(grantWrite != null){
                        for (TransactionId grantTid: grantWrite){
                            if(grantTid.equals(tid)){
                                return true;
                            }
                            queue.add(grantTid);
                        }
                    }
                }
            }

            if(writePage != null){
                for (PageId page: writePage){
                    Set<TransactionId> grantRead = grantReads.get(page);
                    Set<TransactionId> grantWrite = grantWrites.get(page);
                    if(grantRead != null){
                        for (TransactionId grantTid: grantRead){
                            if(grantTid.equals(tid)){
                                return true;
                            }
                            queue.add(grantTid);
                        }
                    }
                    if(grantWrite != null){
                        for (TransactionId grantTid: grantWrite){
                            if(grantTid.equals(tid)){
                                return true;
                            }
                            queue.add(grantTid);
                        }
                    }
                }
            }
        }

        return false;
    }


    public boolean holdsLock(TransactionId tid, PageId pid){
        TransactionId writeLock = writeLocks.get(pid);
        Set<TransactionId> readLock = readLocks.get(pid);
        if(writeLock == tid || (readLock != null && readLock.contains(tid))){
            return true;
        }
        return false;
    }


    public void releaseLock(TransactionId tid, PageId pid) {
        Set<PageId> readPage = readPages.get(tid);
        Set<PageId> writePage = writePages.get(tid);
        Set<TransactionId> readLock = readLocks.get(pid);
        TransactionId writeLock = writeLocks.get(pid);
        if(readPage != null){
            readPage.remove(pid);
            readPages.put(tid, readPage);
        }
        if(writePage != null){
            writePage.remove(pid);
            writePages.put(tid, writePage);
        }
        if(readLock != null){
            readLock.remove(tid);
            readLocks.put(pid, readLock);
        }
        if(writeLock == tid){
            writeLocks.remove(pid, tid);
        }

    }

    public void releaseTidLock(TransactionId tid){
        Set<PageId> readPage = readPages.get(tid);
        Set<PageId> writePage = writePages.get(tid);
        if(readPage != null){
            for (PageId page: readPage){
                Set<TransactionId> readLock = readLocks.get(page);
                readLock.remove(tid);
                readLocks.put(page, readLock);
            }
            readPages.remove(tid);
        }
        if(writePage != null){
            for (PageId page: writePage){
                writeLocks.remove(page);
            }
            writePages.remove(tid);
        }
    }



    public boolean grantLock(TransactionId tid, PageId pid, Permissions perm, boolean firstTime){
        if(perm.equals(Permissions.READ_ONLY)){
            Set<TransactionId> readLock = readLocks.get(pid);
            TransactionId writeLock = writeLocks.get(pid);
            if(writeLock == null || writeLock.equals(tid)){
                if(readLock == null){
                    readLock = new HashSet<TransactionId>();
                    readLocks.put(pid, readLock);
                }
                readLock.add(tid);

                Set<PageId> readPage = readPages.get(tid);
                if(readPage == null){
                    readPage = new HashSet<PageId>();
                    readPages.put(tid, readPage);
                }
                readPage.add(pid);

                if(!firstTime){
                    Set<TransactionId> grantRead = grantReads.get(pid);
                    grantRead.remove(tid);
                    grantReads.put(pid, grantRead);
                }
                return true;
            }
            else{
                if(firstTime) {
                    Set<TransactionId> grantRead = grantReads.get(pid);
                    if (grantRead == null) {
                        grantRead = new HashSet<TransactionId>();
                    }
                    grantRead.add(tid);
                    grantReads.put(pid, grantRead);
                }
                return false;
            }
        }
        else{
            Set<TransactionId> readLock = readLocks.get(pid);
            TransactionId writeLock = writeLocks.get(pid);
            if(readLock != null){
                if(readLock.size() > 1 || (readLock.size() == 1 && !readLock.contains(tid))){
                    if(firstTime) {
                        Set<TransactionId> grantWrite = grantWrites.get(pid);
                        if (grantWrite == null) {
                            grantWrite = new HashSet<TransactionId>();
                        }
                        grantWrite.add(tid);
                        grantWrites.put(pid, grantWrite);
                    }
                    return false;
                }
            }
            if(writeLock != null && !writeLock.equals(tid)){
                if(firstTime) {
                    Set<TransactionId> grantWrite = grantWrites.get(pid);
                    if (grantWrite == null) {
                        grantWrite = new HashSet<TransactionId>();
                    }
                    grantWrite.add(tid);
                    grantWrites.put(pid, grantWrite);
                }
                return false;
            }
            writeLocks.put(pid, tid);
            Set<PageId> writePage = writePages.get(tid);
            if(writePage == null){
                writePage = new HashSet<PageId>();
                writePages.put(tid, writePage);
            }
            writePage.add(pid);
            if(!firstTime){
                Set<TransactionId> grantWrite = grantWrites.get(pid);
                grantWrite.remove(tid);
                grantWrites.put(pid, grantWrite);
            }
            return true;
        }
    }

}
