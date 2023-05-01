//Names: Michael Spero (9081209216) Ali Alawami (9081316052) Alex Carlson (9081313620)
//The "Heart of the Bugger manager". This is where the important functions of the Clock Algorithm are stored
//The allocBuf(), readPage(), unPinPage(), allocPage() are the four functions we worked on.

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**This function allocates a spot on the bufferPool for an incomiing page
the function advances forward to a new frame if the one being examined either has a pin count>0 or also has a reference bit set to 1
If none of the other conditions hold, the frame is then allocated for the incoming page
@param frame = allocated frame is returned via this varaible.
@return OK if no errors, BUFFEREXCEEDED if all buffer frames are pinnnd or UNIXXER if the call to the I/O later returned an error.
const Status BufMgr::allocBuf(int & frame) 
**/
const Status BufMgr::allocBuf(int & frame) 
{
    int count = 0;
    int tmpFrame = -1;
    while(count < numBufs){
        if(bufTable[clockHand].pinCnt > 0){
            advanceClock();
        }else if(bufTable[clockHand].refbit){
            bufTable[clockHand].refbit = false;
            if(tmpFrame == -1){
                tmpFrame = clockHand;
            }
            advanceClock();
        }else{
            frame = clockHand;

            if(bufTable[clockHand].dirty){
                // if dirty page then write to disk
                // Handle case if return is UNIXXER
                Status status1 = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
                if(status1 != OK){
                    return status1;
                }
            }

            if(bufTable[clockHand].valid){
                // Remove entry from hash table
                Status status2  = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                if(status2 != OK){
                    return status2;
                }
            }

            // Clear sets dirty and refbit to false, pincnt to 0, file to null, pageno to -1, valid to false
            bufTable[clockHand].Clear();
            return OK;
        }
        count += 1;
    }

    if(tmpFrame != -1){
        // do same as else above
        frame = tmpFrame;
        clockHand = tmpFrame;

        if(bufTable[clockHand].dirty){
            // if dirty page then write to disk
            // Check case when UNIXXER
            Status status3 = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
            if(status3 != OK){
                return status3;
            }
        }

        if(bufTable[clockHand].valid){
            // Remove entry from hash table
            Status status4 = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
              if(status4 != OK){
                return status4;
            }
        }

        // Clear sets dirty and refbit to false, pincnt to 0, file to null, pageno to -1, valid to false
        bufTable[clockHand].Clear();
        return OK;

    }

    // Potentially throw the error as well
    return BUFFEREXCEEDED;

}


/** This function handles when a page from the file is being requested
 * The function first looks for whether the page already exitst in the bufferpool
 * If so, the pc for the given page is incremented and a pointer to the frame containing the given page is returned
 * If not, the buffer is allocated to make room for the new page
 * @param file the file object, pageNo: the page number, Page: the reference to the page to be returned
 * @return Ok if no errors UNIXXER if a unix error occured, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if hash table error
 * */ 
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    
    if(status == HASHNOTFOUND){
        // Case 1: Not in lookup table. Hash Not Found Error caught
        Status status2 = allocBuf(frameNo);// BUFFEREXCEEDED or UNIXXER
        if(status2 != OK){
            return status2;
        }

        Status status3 = file->readPage(PageNo, &bufPool[frameNo]);
        if(status3 != OK){
            return status3;
        }

        Status status4 = hashTable->insert(file, PageNo, frameNo);
        if(status4 != OK){
            return status4;
        }
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];// Something with frameNo

    }
    else{

        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt += 1;
        page = &bufPool[frameNo];// Something with frameNo
        // Add HASHNOTFOUND error catch


    }

    return OK;

}



/**
 * This function decrements the pin count of a page when called and sets the dirty bit to true if needed
 * @param file the file object, pageNo: the page number, dirty: true if page has been modified, false if not
 * @return OK if no errors PAGENOTPINNED if page's pin count is already 0 
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo = -1;
    Status status1 = hashTable->lookup(file, PageNo, frameNo);

    if(status1 != OK){
        return status1;
    }

    if(bufTable[frameNo].pinCnt == 0){
        return PAGENOTPINNED;
    }

    if(dirty){
        bufTable[frameNo].dirty = true;
    }

    bufTable[frameNo].pinCnt -= 1;

    return OK;
}


/**
 * This method allocates a page to be added into the buffer pool
 * The method first allocates the new page from the file and then allocates the buffer to be prepared for the page
 * The page is then inserted into the buffer pool
 *  @param file the file object, pageNo: the page number, Page: the reference to the page to be returned
 * @return OK if no errors UNIXXER if a unix error occured, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if hash table error
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    int frameNo = -1;
    Status status1 = file->allocatePage(pageNo);
    if(status1 != OK){
        return status1;
    }
    Status status2  = allocBuf(frameNo);
    if(status2 != OK){
        return status2;
    }

    Status status3 = hashTable->insert(file, pageNo, frameNo);
    if(status3 != OK){
        return status3;
    }
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    return OK;


}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file)
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


