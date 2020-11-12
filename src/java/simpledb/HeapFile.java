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

    private File _file;
    private TupleDesc _td;
    private int _numPages;
    private long _fileSize;

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
        _fileSize = f.length();
        this._numPages = (int) Math.ceil(_fileSize/BufferPool.getPageSize());
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
        try {
            RandomAccessFile fi = new RandomAccessFile(_file, "r");
            byte[] data = new byte[BufferPool.getPageSize()];
            int readLen = Math.min(BufferPool.getPageSize(), (int)(_fileSize - fileOffset));
            fi.seek(fileOffset);
            fi.read(data, 0, readLen);
            fi.close();

            return new HeapPage(hpid, data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId hpid = (HeapPageId) page.getId();
        int pgNo = hpid.pageNumber();
        int fileOffset = pgNo * BufferPool.getPageSize();
        try {
            RandomAccessFile fi = new RandomAccessFile(_file, "rw");
            byte[] data = page.getPageData();
            fi.seek(fileOffset);
            fi.write(data);
            fi.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return _numPages;
    }

    protected void setNumPages(int num) {
        _numPages = num;
    }

    protected void setFileSize(long size) {
        _fileSize = size;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        ArrayList<Page> dirtied_pages = new ArrayList<>();

        // Find next available page in file
        for (int i = 0; i < _numPages; i++) {
            HeapPageId hpid = new HeapPageId(getId(), i);
            HeapPage curpage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
            // Debug.log("curpage(%s) avail slots: (%d/%d)", curpage.getId(), curpage.getNumEmptySlots(), curpage.numSlots);
            if (curpage.getNumEmptySlots() > 0) {
                curpage.insertTuple(t);
                dirtied_pages.add(curpage);
                break;
            }
        }

        // If can't find available page in file
        if (dirtied_pages.isEmpty()) {

            // create a new page and insert tuple
            HeapPageId newhpid = new HeapPageId(getId(), _numPages);
            HeapPage newpage = new HeapPage(newhpid, HeapPage.createEmptyPageData());
            newpage.insertTuple(t);

            // Append it to the physical file on disk
            writePage(newpage);

            // Update numpages and filesize accordingly
            _fileSize = _file.length();
            _numPages = (int) Math.ceil(_fileSize/BufferPool.getPageSize());

            // Add to dirtied_pages list
            dirtied_pages.add(newpage);

            Debug.log("Writing to a new page! (new pg count: %d, %d)", _numPages, numPages());
        }

        return dirtied_pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        ArrayList<Page> dirtied_pages = new ArrayList<>();

        // Traverse page to find page that contains the tuple to delete
        for (int i = 0; i < _numPages; i++) {
            HeapPageId hpid = new HeapPageId(getId(), i);
            HeapPage curpage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
            try {
                curpage.deleteTuple(t);
                dirtied_pages.add(curpage);
                return dirtied_pages;
            } catch (DbException e) {
                continue;
            }
        }

        if (dirtied_pages.isEmpty())
            throw new DbException(String.format("Tuple(%s) cannot be find anywhere in HeapFile(%s)", t.toString(), getId()));

        return dirtied_pages;
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
                HeapPageId hpid = new HeapPageId(getId(), 0);
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

