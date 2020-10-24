package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    File file;
    TupleDesc tupleDesc;
    private int numPage;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        numPage = (int) file.length() / BufferPool.getPageSize();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
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
        try {
            RandomAccessFile file = new RandomAccessFile(this.file, "r");
            byte[] data = new byte[BufferPool.getPageSize()];

            file.seek(pid.getPageNumber() * BufferPool.getPageSize());
            file.read(data, 0, BufferPool.getPageSize());

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile file = new RandomAccessFile(this.file, "rw");
        file.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage heapPage = (HeapPage) getInsertablePage(tid);
        heapPage.insertTuple(t);
        heapPage.markDirty(true, tid);
        ArrayList<Page> arrayList = new ArrayList<>(4);
        arrayList.add(heapPage);
        return arrayList;
    }

    // TODO: 2020/10/22  this new page is not in buffer pool and not in hard disk
    private Page getInsertablePage(TransactionId transactionId) throws TransactionAbortedException, DbException, IOException {
        for (int i = 0; i < numPage; i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() != 0) {
                return heapPage;
            }
        }
        return createBlankPage(transactionId);
    }


    private Page createBlankPage(TransactionId transactionId) throws IOException, TransactionAbortedException, DbException {
        HeapPageId heapPageId = new HeapPageId(getId(), numPage);
        numPage++;
        HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        // dump to disk, so the buffer pool can get it from disk through readPage
        writePage(heapPage);
        heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_WRITE);
        return heapPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> arrayList = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        arrayList.add(heapPage);
        heapPage.markDirty(true, tid);
        return arrayList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            int pageNo;
            Iterator<Tuple> iterator;

            Iterator<Tuple> getTupleInPage(HeapPageId heapPageId) throws TransactionAbortedException, DbException {
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNo = 0;
                HeapPageId pageId = new HeapPageId(getId(), pageNo);
                iterator = getTupleInPage(pageId);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) {
                    return false;
                }
                if (iterator.hasNext()) {
                    return true;
                }
                while (pageNo < numPage - 1) {
                    pageNo++;
                    HeapPageId heapPageId = new HeapPageId(getId(), pageNo);
                    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                    iterator = heapPage.iterator();
                    if (iterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pageNo = 0;
                iterator = null;
            }
        };
    }
}

