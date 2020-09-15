package simpledb;

import java.io.*;
import java.nio.Buffer;
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

    // pageid -> offset?
    private File _file;
    private TupleDesc _td;
    private int _numPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this._file = f;
        this._td = td;
        // precalculate number of pages
        long pageSize = BufferPool.getPageSize();
        long fileSize = f.length();
        this._numPages = (int) Math.ceil(fileSize/pageSize);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return _file;
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
        return _file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId hpid = (HeapPageId) pid;
        int pgNo = hpid.pageNumber();
        int fileOffset = pgNo * BufferPool.getPageSize();
        FileInputStream fi;
        try {
            fi = new FileInputStream(_file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }

        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            fi.read(data, fileOffset, BufferPool.getPageSize());
            return new HeapPage(hpid, data);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
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
        // some code goes here
        return _numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        return null;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private boolean _isOpen = false;
            private HeapPage _curPage = null;
            private Iterator<Tuple> _cursor = null;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                _isOpen = true;
                HeapPageId hpid = new HeapPageId(getId(), 0);
                _curPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                _cursor = _curPage.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {

                if ((!_isOpen) || (_curPage == null)) return false;

                // Check if we can find the next tuple in current page
                if (_cursor.hasNext()) return true;
                else {
                    // Check if we have a next page, and then next tuple
                    int nextPageNum = (_curPage.getId().pageNumber() + 1);
                    if (nextPageNum == _numPages)
                        return false;

                    HeapPageId hpid = new HeapPageId(getId(), nextPageNum);
                    _curPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                    _cursor = _curPage.iterator();
                    return _cursor.hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()) {
                    return _cursor.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                HeapPageId hpid = new HeapPageId(getId(), 1);
                _curPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                _cursor = _curPage.iterator();
            }

            @Override
            public void close() {
                _curPage = null;
                _cursor = null;
                _isOpen = false;
            }
        };
    }

}

