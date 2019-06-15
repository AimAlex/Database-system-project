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

    public LockManager(){
        writeLocks = new ConcurrentHashMap<PageId, TransactionId>();
        readLocks = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        readPages = new ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Boolean>>();
        writePages = new ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Boolean>>();
        grantReads = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        grantWrites = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
    }

    synchronized public boolean deadLockOccur(TransactionId tid, PageId pid){
        ConcurrentHashMap<TransactionId, Boolean> visit = new ConcurrentHashMap<TransactionId, Boolean>();

        Queue<TransactionId> queue = new ConcurrentLinkedQueue<>();

        queue.add(tid);

        synchronized (this) {
            System.out.print(tid);
                            System.out.print("***");
                            System.out.print(pid);
                            System.out.print("\n");
                            System.out.print("grantWrites\n");
                            for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantWrites.entrySet()){
                                System.out.print(entry);
                                System.out.print("\n");
                            }
            System.out.print("grantReads\n");
            for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantReads.entrySet()){
                System.out.print(entry);
                System.out.print("\n");
            }
            System.out.print("writeLock\n");
            System.out.print(writeLocks.get(pid));
            System.out.print("\nreadLock\n");
            System.out.print(readLocks.get(pid));
                            System.out.print("\nXXX\n");
//                            System.out.print(grantWrites.entrySet());
                        }
        while(!queue.isEmpty()){

            TransactionId tmp = queue.poll();
//            System.out.print(tmp);
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


    synchronized public boolean holdsLock(TransactionId tid, PageId pid){
        System.out.print("\n Call holdLock \n");
        TransactionId writeLock = writeLocks.get(pid);
        ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
        if(writeLock == tid || (readLock != null && readLock.containsKey(tid))){
            return true;
        }
        return false;
    }


    synchronized public void releaseLock(TransactionId tid, PageId pid) {
        ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);

        System.out.print("\n Call releaseLock \n");

        if(readPage != null){
            readPage.remove(pid);
            readPages.put(tid, readPage);
        }

        ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);
        if(writePage != null){
            writePage.remove(pid);
            writePages.put(tid, writePage);
        }

        ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
        if(readLock != null){
            readLock.remove(tid);
            readLocks.put(pid, readLock);
        }

        TransactionId writeLock = writeLocks.get(pid);
        if(writeLock == tid){
            writeLocks.remove(pid, tid);
        }

    }

    synchronized public void releaseTidLock(TransactionId tid){
        ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);

        if(readPage != null){
            for (PageId page: readPage.keySet()){
                ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(page);
                readLock.remove(tid);
                readLocks.put(page, readLock);
            }
            readPages.remove(tid);
        }

        ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);
        if(writePage != null){
            for (PageId page: writePage.keySet()){
                writeLocks.remove(page);
            }
            writePages.remove(tid);
        }

        System.out.print("\n Call releaseTidLock \n");
        for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantWrites.entrySet()){
            ConcurrentHashMap<TransactionId, Boolean> set = entry.getValue();
            set.remove(tid);
            grantWrites.put(entry.getKey(), set);
        }
        for(Map.Entry<PageId, ConcurrentHashMap<TransactionId, Boolean>> entry: grantReads.entrySet()){
            ConcurrentHashMap<TransactionId, Boolean> set = entry.getValue();
            set.remove(tid);
            grantReads.put(entry.getKey(), set);
        }
    }



    synchronized public boolean grantLock(TransactionId tid, PageId pid, Permissions perm){
        System.out.print("\n Call grantLock \n");
        if(perm.equals(Permissions.READ_ONLY)){

            ConcurrentHashMap<TransactionId, Boolean> grantRead = grantReads.get(pid);
            if (grantRead == null) {
                grantRead = new ConcurrentHashMap<TransactionId, Boolean>();
            }
            grantRead.put(tid, true);
            grantReads.put(pid, grantRead);

            ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
            TransactionId writeLock = writeLocks.get(pid);
            if(writeLock == null || writeLock.equals(tid)){
                if(readLock == null){
                    readLock = new ConcurrentHashMap<TransactionId, Boolean>();
                    readLocks.put(pid, readLock);
                }
                readLock.put(tid, true);

                ConcurrentHashMap<PageId, Boolean> readPage = readPages.get(tid);
                if(readPage == null){
                    readPage = new ConcurrentHashMap<PageId, Boolean>();
                    readPages.put(tid, readPage);
                }
                readPage.put(pid, true);

                grantRead = grantReads.get(pid);
                grantRead.remove(tid);
                grantReads.put(pid, grantRead);

                return true;
            }
            else{

                return false;
            }
        }

        else{

            ConcurrentHashMap<TransactionId, Boolean> grantWrite = grantWrites.get(pid);
            if (grantWrite == null) {
                grantWrite = new ConcurrentHashMap<TransactionId, Boolean>();
            }
            grantWrite.put(tid, true);
            grantWrites.put(pid, grantWrite);

            ConcurrentHashMap<TransactionId, Boolean> readLock = readLocks.get(pid);
            TransactionId writeLock = writeLocks.get(pid);
            if(readLock != null){
                if(readLock.size() > 1 || (readLock.size() == 1 && !readLock.containsKey(tid))){


                    return false;
                }
            }
            if(writeLock != null && !writeLock.equals(tid)){


                return false;
            }

            synchronized (this) {
                System.out.print("\n*****");
                System.out.print(pid);
                System.out.print("*****\n");
                writeLocks.put(pid, tid);
                ConcurrentHashMap<PageId, Boolean> writePage = writePages.get(tid);

                if (writePage == null) {
                    writePage = new ConcurrentHashMap<PageId, Boolean>();
                    writePages.put(tid, writePage);
                }
                writePage.put(pid, true);
            }

            grantWrite = grantWrites.get(pid);
            grantWrite.remove(tid);
            grantWrites.put(pid, grantWrite);
            return true;
        }
    }

}
