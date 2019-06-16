package simpledb;


import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LockManager {
    private Map<PageId, TransactionId> writeLocks;
    private Map<PageId, ConcurrentHashMap<TransactionId, Boolean>> readLocks;
    private Map<TransactionId, ConcurrentHashMap<PageId, Boolean>> readPages;
    private Map<TransactionId, ConcurrentHashMap<PageId, Boolean>> writePages;
    private Map<PageId, ConcurrentHashMap<TransactionId, Boolean>> grantWrites;
    private Map<PageId, ConcurrentHashMap<TransactionId, Boolean>> grantReads;
    private Map<PageId, Object> lockMap;

    public LockManager(){
        writeLocks = new ConcurrentHashMap<PageId, TransactionId>();
        readLocks = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        readPages = new ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Boolean>>();
        writePages = new ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Boolean>>();
        grantReads = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        grantWrites = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        lockMap = new ConcurrentHashMap<PageId, Object>();
    }

    public boolean deadLockOccur(TransactionId tid, PageId pid){
        ConcurrentHashMap<TransactionId, Boolean> visit = new ConcurrentHashMap<TransactionId, Boolean>();

        Queue<TransactionId> queue = new ConcurrentLinkedQueue<>();

        queue.add(tid);

//        synchronized (this) {
//            //System.out.print(tid);
//                            //System.out.print("***");
//                            //System.out.print(pid);
//                            //System.out.print("\n");
//                            //System.out.print("grantWrites\n");
//                            for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantWrites.entrySet()){
//                                //System.out.print(entry);
//                                //System.out.print("\n");
//                            }
//            ////System.out.print("grantReads\n");
//            for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantReads.entrySet()){
//                //System.out.print(entry);
//                //System.out.print("\n");
//            }
//            //System.out.print("writeLock\n");
//            //System.out.print(writeLocks.get(pid));
//            //System.out.print("\nreadLock\n");
//            //System.out.print(readLocks.get(pid));
//                            //System.out.print("\nXXX\n");
////                            //System.out.print(grantWrites.entrySet());
//                        }
        while(!queue.isEmpty()){

            TransactionId tmp = queue.poll();
//            //System.out.print(tmp);
            if(visit.containsKey(tmp)){
                continue;
            }
            visit.put(tmp, true);


            ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tmp);

            if(readPage != null){
                for (PageId page: readPage.keySet()){
                    ConcurrentHashMap<TransactionId, Boolean> grantWrite = grantWrites.get(page);
                    if(grantWrite != null){
                        for (TransactionId grantTid: grantWrite.keySet()){
                            if(grantTid.equals(tid) && !tmp.equals(tid)){
                                grantWrite.remove(tid);
                                return true;
                            }
                            queue.add(grantTid);
                        }
                    }
                }
            }

            ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tmp);
            if(writePage != null){
                for (PageId page: writePage.keySet()){
                    ConcurrentHashMap<TransactionId, Boolean> grantRead = grantReads.get(page);
                    ConcurrentHashMap<TransactionId, Boolean> grantWrite = grantWrites.get(page);
                    if(grantRead != null){
                        for (TransactionId grantTid: grantRead.keySet()){
                            if(grantTid.equals(tid)){
                                grantRead.remove(tid);
                                return true;
                            }
                            queue.add(grantTid);
                        }
                    }
                    if(grantWrite != null){
                        for (TransactionId grantTid: grantWrite.keySet()){
                            if(grantTid.equals(tid)){
                                grantWrite.remove(tid);
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
        //System.out.print("\n Call holdLock \n");
        Object lock = lockMap.get(pid);
        synchronized (lock) {
            TransactionId writeLock = writeLocks.get(pid);
            ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
            if (writeLock == tid || (readLock != null && readLock.containsKey(tid))) {
                return true;
            }
            return false;
        }
    }

    public boolean holdsWriteLock(TransactionId tid, PageId pid){
        Object lock = lockMap.get(pid);
        synchronized (lock) {
            TransactionId writeLock = writeLocks.get(pid);
            if (writeLock == tid) {
                return true;
            }
            return false;
        }
    }


    public void releaseLock(TransactionId tid, PageId pid) {
        Object lock = lockMap.get(pid);
//        System.out.print(lock);
        synchronized (lock) {
            ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);

            //System.out.print("\n Call releaseLock \n");

            if (readPage != null) {
                readPage.remove(pid);
//                readPages.put(tid, readPage);
            }

            ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);
            if (writePage != null) {
                writePage.remove(pid);
//                writePages.put(tid, writePage);
            }

            ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
            if (readLock != null) {
                readLock.remove(tid);
//                readLocks.put(pid, readLock);
            }

            TransactionId writeLock = writeLocks.get(pid);
            if (writeLock == tid) {
                writeLocks.remove(pid, tid);
            }
        }

    }

    public void releaseTidLock(TransactionId tid){
        ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);

        if(readPage != null){
            readPages.remove(tid);
            for (PageId page: readPage.keySet()){
                if(!lockMap.containsKey(page)){
                    Object lock = new Object();
                    lockMap.put(page, lock);
                }
                Object lock = lockMap.get(page);
                synchronized (lock) {
                    ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(page);
                    readLock.remove(tid);
//                    readLocks.put(page, readLock);
                }
            }
        }

        ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);
        if(writePage != null){
            writePages.remove(tid);
            for (PageId page: writePage.keySet()){
                if(!lockMap.containsKey(page)){
                    Object lock = new Object();
                    lockMap.put(page, lock);
                }
                Object lock = lockMap.get(page);
                synchronized (lock) {
                    writeLocks.remove(page);
                }
            }

        }

        //System.out.print("\n Call releaseTidLock \n");
        for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantWrites.entrySet()){
            if(!lockMap.containsKey(entry.getKey())){
                Object lock = new Object();
                lockMap.put(entry.getKey(), lock);
            }
            Object lock = lockMap.get(entry.getKey());
            synchronized (lock) {

                ConcurrentHashMap<TransactionId, Boolean> set = entry.getValue();
                set.remove(tid);
//                grantWrites.put(entry.getKey(), set);
            }
        }
        for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantReads.entrySet()){
            Object lock = lockMap.get(entry.getKey());
            synchronized (lock) {
                ConcurrentHashMap<TransactionId, Boolean> set = entry.getValue();
                set.remove(tid);
//                grantReads.put(entry.getKey(), set);
            }
        }
    }



    public boolean grantLock(TransactionId tid, PageId pid, Permissions perm){
        //System.out.print("\n Call grantLock \n");
        if(!lockMap.containsKey(pid)){
            Object lock = new Object();
            lockMap.put(pid, lock);
        }

        Object lock = lockMap.get(pid);

        synchronized (lock) {
            if (perm.equals(Permissions.READ_ONLY)) {

                ConcurrentHashMap<TransactionId, Boolean> grantRead = grantReads.get(pid);
                if (grantRead == null) {
                    grantRead = new ConcurrentHashMap<TransactionId, Boolean>();
                    grantReads.put(pid, grantRead);
                }
                grantRead.put(tid, true);


                ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
                TransactionId writeLock = writeLocks.get(pid);
                if (writeLock == null || writeLock.equals(tid)) {

//                    if (writeLock != null){
//                        writeLocks.remove(pid);
//                        ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);
//                        writePage.remove(pid);
////                        writePages.put(tid, writePage);
//                    }

                    if (readLock == null) {
                        readLock = new ConcurrentHashMap<TransactionId, Boolean>();
                        readLocks.put(pid, readLock);
                    }
                    readLock.put(tid, true);


                    ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);
                    if (readPage == null) {
                        readPage = new ConcurrentHashMap<PageId, Boolean>();
                        readPages.put(tid, readPage);

                    }
                    readPage.put(pid, true);


                    grantRead = grantReads.get(pid);
                    grantRead.remove(tid);
//                    grantReads.put(pid, grantRead);

                    return true;
                } else {

                    return false;
                }
            }
            else {

                ConcurrentHashMap<TransactionId, Boolean> grantWrite = grantWrites.get(pid);
                if (grantWrite == null) {
                    grantWrite = new ConcurrentHashMap<TransactionId, Boolean>();
                    grantWrites.put(pid, grantWrite);
                }
                grantWrite.put(tid, true);


                ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
                TransactionId writeLock = writeLocks.get(pid);
                if (readLock != null && readLock.size() > 0) {
                    if (readLock.size() > 1 || (readLock.size() == 1 && !readLock.containsKey(tid))) {


                        return false;
                    }

                }
                if (writeLock != null && !writeLock.equals(tid)) {


                    return false;
                }

                if(readLock != null && readLock.size() == 1){
                    readLock.remove(tid);
//                    readLocks.put(pid, readLock);

                    ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);
                    readPage.remove(pid);
//                    readPages.put(tid, readPage);
                }
//            synchronized (this) {
                //System.out.print("\n*****");
                //System.out.print(pid);
                //System.out.print("*****\n");

                writeLocks.put(pid, tid);

                ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);

                if (writePage == null) {
                    writePage = new ConcurrentHashMap<PageId, Boolean>();
                    writePages.put(tid, writePage);
                }
                writePage.put(pid, true);
//
//            }

                grantWrite = grantWrites.get(pid);
                grantWrite.remove(tid);
//                grantWrites.put(pid, grantWrite);
                return true;
            }
        }
    }

}
