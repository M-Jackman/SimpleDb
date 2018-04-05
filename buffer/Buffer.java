package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;
import java.util.Date;


/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 */
public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   private int modifiedBy = -1;  // negative means not modified
   private int logSequenceNumber = -1; // negative means no corresponding log record

   /** CS4432-Project1:
    *  Added three new variables
    *  bufferLocation is the location where buffer is. It is initalized at -1 representing nowhere in the buffer
    *  lastAccess is the last time a buffer was accessed, using the java dates
    *  referance is used for clock replacement
    */
   private int bufferLocation = -1 ; // -1 means not in bufferpool
   private Date lastAccess;
   private int referance = 0; // used for clock replacement

   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    */
   public Buffer() {
      // CS4432-Project1:
      // baseline date is it's creation
      lastAccess = new Date() ;
   }


    //CS4432-Project1:
    //Returns the location of the buffer
   public Integer getbufferLocation(){
      if (bufferLocation != -1){
         return bufferLocation;
      }

      return null;

   }

   //CS4432-Project1:
   //this lets you edit the buffer locations
   public void changebufferLocation(int i){
      bufferLocation = i ;
   }

   // CS4432-Project1:
   // Function for updating the last access, important to do in LRU
   public void updateAccess() {
      lastAccess = new Date();
   }

   //CS4432-Project1:
   //this returns the date accessed
   // This is also only useful in LRU
   public Date getLastAccess() {
      return lastAccess;
   }


   //CS4432-Project1:
   // Controls the referance bit. Used for clock policy
   public void changeRef(int i) {
      referance = i ;
   }

   // CS4432-Project1:
   // get referance is used to get the referance.
   // Is useful for clock policy
   public int getReferance() {
      return referance ;
   }


   /**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);

      //CS4432-Project1:
      //Whenever this is changed, a new date is needed. This is only important in the LRU portion
      updateAccess() ;
   }

   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);

      //CS4432-Project1:
      // Again, whenever you do anything, you update when the buffer was last accessed
      updateAccess() ;
   }

   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
      return blk;
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    */
   void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
   }

   /**
    * Increases the buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decreases the buffer's pin count.
    */
   void unpin() {
      pins--;
   }

   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   boolean isPinned() {
      return pins > 0;
   }

   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;

      // CS4432-Project1:
      // Whenever you do anything, you need to update access
      updateAccess() ;
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;

      //CS4432-Project1:
      // Last but not least, you need to update last used when you do anything
      // Used for LRu
      updateAccess() ;
   }


   /**
    * CS4432-Project1:
    * Returns information about the buffer's id, block, and pin status
    */
   public String toString(){
      Block block = blk ;

      String info = "Buffer: " + bufferLocation + ", Pin: " + pins + ", Block: ";
      if (block != null){
         info = info + block.toString();
      } if (block == null) {
         info = info + "Not currently assigned block";
      }

      return info;
   }
}