///////////////////////////////////////////////////////////////////////////////
//                 
// Title:            heapfile.c
// Semester:         Fall 2022
//
// Names(IDs):       Michael Spero (9081209216), Alex Carlson (9081313620) Ali Alawami (9081316052)
//
//////////////////////////// 80 columns wide //////////////////////////////////

#include "heapfile.h"
#include "error.h"




/*
This function creates an empty heap file. 
We first check to see if the given file is already open, if it is open we return file exists already
If it is not open already we create a new file using the file name and open it assigning it to the file pointer
We then allocate pages for both the header and the first page of the heapfile
We update the values of the headerpage 
Finally we unpin those pages, flush and close the file 
@param fileName, the name of the heapfile to be created
@return OK if heapfile is allocated, fileExists is file already exists  

*/
const Status createHeapFile(const string fileName)
{
    File*     file;
    Status        status;
    FileHdrPage*   hdrPage;
    int          hdrPageNo;
    int          newPageNo;
    Page*     newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {

      // file doesn't exist. First create it and allocate
      // an empty header page and data page.
      Status status1 = db.createFile(fileName);
        if (status1 != OK){
            return status1;
        }

    

      Status status5 = db.openFile(fileName, file);
      if(status5 != OK){
        return status5;
      }

      //use newPage
       // Page* tmpPage;

        // First Call
        Status status2 = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status2 != OK){
            return status2;
        }

       


        hdrPage = (FileHdrPage*) newPage;

 
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);



      // Second Call
        Status status3 = bufMgr->allocPage(file, newPageNo, newPage);
        if (status3 != OK){
            return status3;
        }



        newPage->init(newPageNo);
        status = newPage->setNextPage(-1);

        // Initializing Values
        hdrPage->recCnt = 0;
        hdrPage->pageCnt = 1;
        




        // Initialize more values
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

        // Unpin both pages and mark as dirty
       Status status6 = bufMgr->unPinPage(file, hdrPageNo, true);
         if (status6 != OK){
            return status6;
        }
        Status status7 = bufMgr->unPinPage(file, newPageNo, true);
         if (status7 != OK){
            return status7;
        }
        status = bufMgr->flushFile(file);
        if(status != OK){
            return status;
        }

        status = db.closeFile(file);
        if(status != OK){
            return status;
        }

        else{
            return OK;
        }

    }
    return (FILEEXISTS);
}


// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)

{
	return (db.destroyFile (fileName));
}



/*
This function opens the file given by the fileName. If it is unable to be opened, BADFILE is returned 
This function reads and pins the header page into the pool 
The function then reads the first page into the pool 
*/
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {

        //reads and pins the header page for the file in the buffer pool
        status = filePtr->getFirstPage(headerPageNo);
         if(status != OK){
            returnStatus = status;
        }
        status = bufMgr->readPage(filePtr,headerPageNo,pagePtr);
        if(status != OK){
            returnStatus = status;
        }
    

       //initializing the private data members headerPage, headerPageNo, and hdrDirtyFlag
        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;


        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if(status != OK){
		    returnStatus = status;
        }

        
        //initializing the values of curPage, curPageNo, and curDirtyFlag appropriately
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = OK;


        return;

        
		
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.

// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status1;
    Status status2;
    Status status3;


   // Check if rids page number is on currently pinned page
   if (rid.pageNo != curPageNo){
       // else unpin page then read page into buffer pool
       status2 = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
       if (status2 != OK){
           return status2;
       }
       status3 = bufMgr->readPage(filePtr, rid.pageNo, curPage);
       if (status3 != OK){
           return status3;
       }
       curPageNo = rid.pageNo;
   }


    status1 = curPage->getRecord(rid, rec);
   
   return status1;
   
   
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}



/*Used as an iterator to go through the file and returns the next record that matches the filter
//The function returns OK if there is a match and assings outRid to the matching RID 
//The funciton returns FILEEOF if the file has been scanned through and no records were found 
@param outRID the rid to be returned if there is a match
@return OK if match is found, FILEEOF if no match is found 
*/
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status     status = OK;
    RID       nextRid;
    RID       tmpRid;
    int    nextPageNo;
    Record      rec;


    //Checks if our current record exists
   bool start = true;
    if (curRec.pageNo == -1){
        //If no curr record set, get the first record from the currPage
        status = curPage->firstRecord(nextRid);
        
    }
    else{
       status = curPage->nextRecord(curRec, nextRid);
    }

    while(1){
    if(start != true){
    status = curPage->nextRecord(tmpRid,nextRid);
    }
    else{
        start = false;
    }
    while(status == NORECORDS || status == ENDOFPAGE){
        curPage->getNextPage(nextPageNo);
        if(nextPageNo == -1){
            return FILEEOF;
        }
        else{
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        bufMgr->readPage(filePtr, nextPageNo, curPage);
        curPageNo = nextPageNo;
        curDirtyFlag = false;
        status = curPage->firstRecord(nextRid);
        }
    }

    tmpRid = nextRid;

    curPage->getRecord(tmpRid,rec);

    if (matchRec(rec)){
            outRid = tmpRid;
            curRec = tmpRid;
            return OK;
    }
    }

    
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
    
}

InsertFileScan::~InsertFileScan()
{


    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// This function inserts a given record into the heapfile
//The function looks for an open page or else it creates a new page 
//The record is then inserted on the new page and the header page is updated 
//@param rec the record to be added and outRid the rid of that record
//@return OK if recorc was added succusfully, INVALIDRECLEN if there is an invald record length or errors generated by the bufMgr when allocating pages
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;


    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    status = curPage->insertRecord(rec,outRid);

    if(status == NOSPACE){


        Status status1 = bufMgr->allocPage(filePtr,newPageNo,newPage);
        if(status1 != OK){
            return status1;
        }

        //initialize new page 
        newPage->init(newPageNo);


        //sets the currentNextpage to correct page 
        status1 = curPage->setNextPage(newPageNo);
        if(status1 != OK){
            return status1;
        }

      
        unpinstatus = bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
     
        // bufMgr->disposePage(filePtr,curPageNo);
         if(unpinstatus != OK){
            return unpinstatus;
        }

        //inserts record into new
        newPage->insertRecord(rec,outRid);
        headerPage->lastPage = newPageNo;  

        //assigns new currentpage correct variables
        curPage = newPage;
        curPageNo = newPageNo;


        //increases pageCount by1
        headerPage->pageCnt +=1; 
        
       

   
    }
    

    
    headerPage->recCnt+=1;
    curDirtyFlag = true;
    hdrDirtyFlag = true;
    return OK;

    
 
   


  
  
}


