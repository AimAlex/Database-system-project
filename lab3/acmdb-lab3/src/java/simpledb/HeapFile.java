package simpledb;

import javax.sound.midi.Track;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.pageNumber();
        int offset = pgNo * BufferPool.getPageSize();
        try{
            RandomAccessFile File = new RandomAccessFile(file, "r");
            byte[] Byte = new byte[BufferPool.getPageSize()];
            File.seek(offset);
            File.read(Byte, 0, BufferPool.getPageSize());
            HeapPageId hpid = (HeapPageId) pid;
            return new HeapPage(hpid, Byte);
        } catch (IOException err){
            err.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)Math.ceil(file.length() / BufferPool.getPageSize());
        // some code goes here
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<Page>();

        for(int i = 0; i < numPages(); ++i) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                pages.add(page);
                return pages;
            }
        }


        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        pages.add(page);

        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        rf.seek(pid.pageNumber() * BufferPool.getPageSize());
        rf.write(page.getPageData(), 0, BufferPool.getPageSize());
        rf.close();


        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> pages= new ArrayList<Page>();
        pages.add(page);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(tid);
    }
    private class HeapFileIterator implements DbFileIterator {
        private int pgNo;
        private Iterator<Tuple> tuples;
        private TransactionId tid;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        private Iterator<Tuple> getTuple(HeapPageId pid) throws TransactionAbortedException, DbException {
             HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
             return hp.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgNo = 0;
            HeapPageId pid = new HeapPageId(getId(), pgNo);
            tuples = getTuple(pid);

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuples == null) return false;

            if (tuples.hasNext()) return true;

            if (pgNo < numPages() - 1) {
                pgNo++;
                HeapPageId pid = new HeapPageId(getId(), pgNo);
                tuples = getTuple(pid);
                return tuples.hasNext();
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException();

            return tuples.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();;
        }

        @Override
        public void close() {
            pgNo = 0;
            tuples = null;
        }
    }
}

