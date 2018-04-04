package simpledb.buffer;

import simpledb.file.*;
import java.util.Arrays;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int[] freeframes ; // Sam Huang: 2.1

   
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

   // Creates a bitmap array to help find free frames. (2.1)
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      freeframes = new int[numbuffs]; // 0 is free, 1 is taken, Sam Huang: 2.1
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         freeframes[i] = 0;
      }
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
   // CS 4432
    // To keep track of which block is pinned,
    // the pin value is checked to see if the file is already pinned or not
    // The pinning mechanism is already set up such that it is not a static boolean value
    // and can be adjusted to comply with section 2.4 from the project document specifications
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer(1); // 1 represents pinning, Sam Huang: 2.1
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         freeframes[buff.getPinlocation()] = 1 ;// Sam Huang: 2.1
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
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
      Buffer buff = chooseUnpinnedBuffer(1); // 1 represents pinning, Sam Huang: 2.1
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      freeframes[buff.getPinlocation()] = 1 ; // Sam Huang: 2.1
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */

    // CS 4432: 2.1
    // When it unpins, you need to change it in the bit array as well as remove the pin location within the buffer class
    // The pin value is treated as a numerical value in this instance
    // to comply with section 2.4 of the project requirements
    // Once the block has been unpinned the counter of available spaces increments
   synchronized void unpin(Buffer buff) {
      int pinLocation = buff.getPinlocation() ;
      freeframes[pinLocation] = 0 ;
      buff.changePinlocation(-1);
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

   // 2.2
   private Buffer findExistingBuffer(Block blk) {
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }

   // 2.1, 2.3
   //
   // Sam Huang: 2.1
   // Implemented the use of a bitmap array to make finding an open buffer space quicker
   // When calling, set pin = 1 when you want to pin the buffer, or pin = 0 for just opening and reading
   private Buffer chooseUnpinnedBuffer(int pin) {
      if (freeframes == null) {
         return null ;
      }
      int bufferLocation = -1;
      for (int i = 0; i < numAvailable; i++) {
         if (freeframes[i] == 0) {
            bufferLocation = i ;
         }
      }

      if (bufferLocation == -1) {
         return null ;
      }

      Buffer buff = bufferpool[bufferLocation] ;
      if (pin == 1) {
         buff.changePinlocation(bufferLocation);
      }

      return buff ;

      //for (Buffer buff : bufferpool)
      //   if (!buff.isPinned())
      //   return buff;
      //return null;
   }

   // Sam Huang: 2.2
   private boolean check_disk_existance(Buffer buff) {
      if (buff.getPinlocation() == -1) {
         return false ;
      }
      else {
         return true ;
      }
   }


}
