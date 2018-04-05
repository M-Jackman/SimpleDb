package simpledb.buffer;

import simpledb.file.*;
import java.util.Hashtable;
import java.util.Scanner;;

import java.util.Date;


/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;

   /**
    * CS4432-Project1
    *  So new variables...
    */
   private int[] freeframes ; // This is a bit array determining where the pinned locations are
   private Hashtable<Block, Integer> currentBuffers; // A list of the used frames. Contains both the location in the buffer pool, and the blk from buffer
   private int replacement_policy ; // LRU vs Clock replacement, upon running create studentDB you will be prompted to choose replacement policy
   private int clockPointer = 0 ; // important for Clock replacement

   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;

      //CS4432-Project1: freeframes refers to unpinned frames
      freeframes = new int[numbuffs]; // 0 is free, 1 is taken
      currentBuffers = new Hashtable<Block, Integer>(numbuffs);
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         freeframes[i] = 0;

      }

      Scanner reader = new Scanner(System.in);  // Reading from System.in
      System.out.println("Choose Replacement Policy, 1 for LRU 2 for clock: ");
      int n = reader.nextInt(); // Scans the next token of the input as an int.
      replacement_policy = n ;
      reader.close();

   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);

         // CS4432-Project1: Add reference to block in table for faster access in future calls to
         // Adding to the hashtable for future lookups HERE
         currentBuffers.put(blk, buff.getbufferLocation());
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();

      // CS4432-Project1
      // Set Ref
      buff.changeRef(1);

      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);



      // CS4432-Project1: Add reference to block in table for faster access in future calls to
      // Adding to the hashtable for future lookups HERE
      currentBuffers.put(buff.block(), buff.getbufferLocation());


      numAvailable--;
      buff.pin();

      // CS4432-Project1
      // Set Ref for clock replacement
      buff.changeRef(1);

      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {

      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;

   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }

   // CS4432-Project1
   // This is where part 2.2 is basically done. Note that the frame num is contained within
   // the buffer object as bufferLocation
   private Buffer findExistingBuffer(Block blk) {

      Integer i = currentBuffers.get(blk) ;

      if (i == null) {
         return null ;
      }

      return bufferpool[i] ;
   }

   // CS4432-Project1
   // LRU Policy.
   public int LRUreplacement(Buffer[] bufferpool) {
      int result = 0;
      int totalSize = bufferpool.length ;
      Date leastRecentlyUsedDate = bufferpool[0].getLastAccess();

      for (int counter = 0; counter < totalSize; counter++) {
         if (bufferpool[counter].isPinned() == false)
            if (bufferpool[counter].getLastAccess() == leastRecentlyUsedDate) {
            result = counter;
         }
      }
      return result; // Return the index of the least recently used buffer
   }

   // CS4432-Project1
   // Clock Policy.
   // Note that the Clock pointer is a global.
   public int Clockreplacement( Buffer[] bufferPool ) {
      int result = -1; // we are going to return this
      Buffer currentBuffer; // So the clock can spin for more than 1 loop, so we are using a while loop here
      int totalBuffers = bufferPool.length ;
      while (result == -1) {
         currentBuffer = bufferPool[ clockPointer ];
         if( currentBuffer.isPinned() == false ) {
            if( currentBuffer.getReferance() == 1 ) {
               currentBuffer.changeRef(0);
            }
            else {
               result = clockPointer; // Selection!
            }
         }
         if (clockPointer > totalBuffers) {
            clockPointer = clockPointer - totalBuffers ;
         }
      }
      return result; // Return the index for the buffer that will be replaced
   }


   // CS4432-Project1
   // Changed most of this function, functionality should still be there
   // When this is run, a buffer location is given to the returned buffer inside the buffer manage, as
   // it is assumed this buffer is going to be used when this function is called
   private Buffer chooseUnpinnedBuffer() {


      if (freeframes == null) {
         return null ;
      }
      int bufferLocation = -1;
      for (int i = 0; i < numAvailable; i++) {
         if (freeframes[i] == 0) {
            bufferLocation = i ;
         }
      }

      if (bufferLocation == -1) { // if there isn't an empty frame...
         if (numAvailable == 0){ // First check if there are unpinned buffers.
            bufferLocation = -1 ;
         } else{

            // CS4432-Project1:
            // Replacement Policy, LRU. Clock is used for least recently use tracker
            // Also, this presupposes there is indeed an unpinned buffer, which should
            // have been checked earlier in this function
            if (replacement_policy == 1) {
               bufferLocation = LRUreplacement(bufferpool) ;
            }
            if (replacement_policy == 2) {
               bufferLocation = Clockreplacement(bufferpool) ;
            }
            else {
               bufferLocation = -1 ;
            }


         }

      }

      if (bufferLocation != -1) { // if there is an empty frame
         Buffer buff = bufferpool[bufferLocation];
         buff.changebufferLocation(bufferLocation);

         //freeframes[bufferLocation] = 0 ;

         if (buff.block() != null) {
            currentBuffers.remove(buff.block());
         }

         return buff;
      }

      else {
         return null ;
      }

   }


   /**
    * CS4432-Project1:
    * Returns information about each buffer's id, block, and pin status
    */
   public String toString(){
      String info = new String();
      for (Buffer buff : bufferpool){
         info = info + buff.toString() + System.getProperty("line.separator");
      }
      return info;
   }
}
