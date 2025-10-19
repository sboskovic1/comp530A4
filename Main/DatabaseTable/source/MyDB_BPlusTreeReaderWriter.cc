
#ifndef BPLUS_C
#define BPLUS_  C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "RecordComparator.h"
#include <algorithm>

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = -1;
	getTable ()->setRootLocation (rootLocation);
	// cout << "Number of pages in table: " << getNumPages() << endl; 
	// cout << "Initiating bplus tree with root location: " << rootLocation << endl;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr lhs, MyDB_AttValPtr rhs) {
    vector<MyDB_PageReaderWriter> pages;
	// printTree();
	// cout << "Searching for lhs of " << lhs->toInt() << endl;
	// cout << "Searching for rhs of " << rhs->toInt() << endl;
    discoverPages(rootLocation, pages, lhs, rhs);

    MyDB_RecordPtr left = getEmptyRecord();
    MyDB_RecordPtr right = getEmptyRecord();

    MyDB_RecordPtr tempRec = getEmptyRecord();

    MyDB_INRecordPtr lhsRec = getINRecord();
    MyDB_INRecordPtr rhsRec = getINRecord();
    lhsRec->setKey(lhs);
    rhsRec->setKey(rhs);
 
    function <bool ()> comparator = buildComparator(left, right);
    function <bool ()> lowComparator = buildComparator(tempRec, lhsRec);
    function <bool ()> highComparator = buildComparator(rhsRec, tempRec);

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, left, right, comparator, tempRec, lowComparator, highComparator, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr lhs, MyDB_AttValPtr rhs) {
    return getSortedRangeIteratorAlt(lhs, rhs);
    // vector<MyDB_PageReaderWriter> pages;
    // discoverPages(rootLocation, pages, lhs, rhs);
    // return make_shared<MyDB_PageListIteratorAlt>(pages);
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> & list, MyDB_AttValPtr lhs, MyDB_AttValPtr rhs) {
    MyDB_RecordPtr currentRec = getEmptyRecord();
    MyDB_PageReaderWriter page = (*this)[whichPage];
    if (page.getType() == MyDB_PageType::RegularPage) {
		// cout << "Adding leaf page with this these values" << endl;
        list.push_back(page);
        return true;
    }
    MyDB_RecordIteratorAltPtr iter = page.getIteratorAlt();
    currentRec = getINRecord();
    MyDB_INRecordPtr left;
    MyDB_INRecordPtr right;
    while (iter->advance()) {
        iter->getCurrent(currentRec);
        MyDB_AttValPtr key = getKey(currentRec);
        left = getINRecord();
        right = getINRecord();
        left->setKey(lhs);
        right->setKey(rhs);
		// cout << "Checking current internal record key: " << currentRec->getAtt (0)->toInt () << endl;
        auto leftCmp = buildEqualToComparator(left, currentRec);
        auto rightCmp = buildComparator(right, currentRec);
        if (leftCmp()) { // Use custom comparator to check if this page should be added
            int idx = static_pointer_cast<MyDB_INRecord>(currentRec)->getPtr();
			// cout << "Going to this page to discover more pages" << endl;
            discoverPages(idx, list, lhs, rhs);
            if (rightCmp()) {
				// cout << "Not going to future pages to discover pages" << endl;
               break;
            }
        }

    }

	return false;
}

// Helper function to print all records
void printRecords(const vector<MyDB_RecordPtr>& records, const string& label) {
    cout << "---- " << label << " ----" << endl;
    for (size_t i = 0; i < records.size(); i++) {
        cout << "Record " << i << ": " << endl;
        cout << records[i];
        cout << endl;
    }
    cout << "-------------------------" << endl;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
	// Base case for an empty tree: Create a internal node (root) with an infinity internal record 
	// that points to an empty leaf page
	if (rootLocation == -1) {
		// cout << "appending record in high level append " << appendMe << endl;
		// cout << "Creating initial root node for b plus tree" << endl;
		// cout << "Initial number of pages: " << this-> getNumPages() << endl;
		rootLocation = 0;
		getTable()->setRootLocation(rootLocation);
		// cout << "Initial root location: " << rootLocation << endl;

		// Create internal node with infinity internal record
		// cout << "Creating initial root page" << endl;
		MyDB_PageReaderWriter rootPage = (*this)[rootLocation];
		rootPage.setType(MyDB_PageType::DirectoryPage);

		// cout << "Creating initial internal record with inifnity key" << endl;
		MyDB_INRecordPtr newINRec = getINRecord();

		// Point root node to an empty leaf page
		int newPageNumber = this->getNumPages();
		// cout << "New page number for leaf page: " << newPageNumber << endl;
		MyDB_PageReaderWriter newPage = (*this)[newPageNumber];
		newPage.setType(MyDB_PageType::RegularPage);
		newINRec->setPtr(newPageNumber);
		
		// cout << "Adding initial internal record to rootPage" << endl;
		// cout << "Initial ptr for root internal record: " << newINRec->getPtr() << endl;
		rootPage.append(newINRec);
		// cout << "Done adding initial internal record to rootPage" << endl;
        // printTree();
	}


	// Create new internal page if root gets split
	// cout << "Current rootLocation: " << rootLocation << endl;
	MyDB_RecordPtr maybeSplit = append(rootLocation, appendMe);
	if (maybeSplit != nullptr) {
		int newPageNumber = this->getNumPages();
		// cout << "Updating root location" << endl;
		// cout << "New page number to add " << newPageNumber << endl;
		MyDB_PageReaderWriter newPage = (*this)[newPageNumber];
		newPage.setType(MyDB_PageType::DirectoryPage);
		newPage.append(maybeSplit);
		// cout << "Created new rootPage and added maybeSplit" << endl;

		// Add a new internal node to point to the old root (key is automatically to largest possible value)
		// cout << "Adding new infinity internal node to root page " << endl;
		MyDB_INRecordPtr newINRec = getINRecord();
		newINRec->setPtr(rootLocation);
		newPage.append(newINRec);
		rootLocation = newPageNumber;
		getTable()->setRootLocation(newPageNumber);
		MyDB_INRecordPtr lhs = getINRecord();
		MyDB_INRecordPtr rhs = getINRecord();
		auto compare = buildComparator(lhs, rhs);
		newPage.sortInPlace(compare, lhs, rhs);
	}

	// cout << "Print tree at end of high level append" << endl;
	// printTree();
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
	// Sort all records (splitMe records + andMe)
	vector<MyDB_RecordPtr> records;
    vector<MyDB_INRecordPtr> inRecords;
	MyDB_RecordPtr tempRec = getEmptyRecord();
    if (splitMe.getType() == MyDB_PageType::DirectoryPage) {
        tempRec = getINRecord();
    }
	MyDB_RecordIteratorPtr iter = splitMe.getIterator(tempRec);
	
	// cout << "Aggregating records together for sorting" << endl;
	while (iter->hasNext()) {
		iter->getNext();
		// cout << "Current record beging sorted: " << tempRec << endl;
		// Deep copy of the record
		// Allocate a temporary buffer for copying the record
		size_t size = tempRec->getBinarySize();
		vector<char> buffer(size);

		// Serialize the other record into the buffer
		tempRec->toBinary(buffer.data());

		// Copy the binary into a new record
        if (splitMe.getType() == MyDB_PageType::DirectoryPage) {
            MyDB_INRecordPtr inRec = getINRecord();
            inRec->fromBinary(buffer.data());
            inRecords.push_back(inRec);
        } else {
            MyDB_RecordPtr recordCopy = getEmptyRecord();
            recordCopy->fromBinary(buffer.data());
            records.push_back(recordCopy);
        }

	}
    if (splitMe.getType() == MyDB_PageType::DirectoryPage) {
        inRecords.push_back(static_pointer_cast<MyDB_INRecord>(andMe));
    } else {
	    records.push_back(andMe);
    }
	// Print before sorting
	// printRecords(records, "Records BEFORE sort");

	if (splitMe.getType() == MyDB_PageType::RegularPage) {
		std::sort(records.begin(), records.end(), [this](MyDB_RecordPtr a, MyDB_RecordPtr b) {
			auto compare = buildComparator(a, b);
			return compare();
		});
	} else {
        std::sort(inRecords.begin(), inRecords.end(), [this](MyDB_INRecordPtr a, MyDB_INRecordPtr b) {
            auto compare = buildComparator(a, b);
            return compare();
        });
        records.clear();
        for (auto &inRec : inRecords) {
            records.push_back(inRec);
        }
    }

	// Print after sorting
	// printRecords(records, "Records AFTER sort");

	// Split records in half
	int mid = records.size() / 2;
	vector<MyDB_RecordPtr> lower(records.begin(), records.begin() + mid);
	vector<MyDB_RecordPtr> upper(records.begin() + mid, records.end());

	// Print both halves for debugging
	// printRecords(lower, "LOWER half (left child)");
	// printRecords(upper, "UPPER half (right child)");

	// Create a new page for the lower half
	int newPageNumber = this->getNumPages();
	MyDB_PageReaderWriter newPage = (*this)[newPageNumber];
	newPage.setType(splitMe.getType());
	// cout << "Finished creating page" << endl;
	newPage.clear();
	for (auto &rec : lower) {
		// cout << "IN lower append" << endl;
		// cout << "Appending in lower half: " << rec << endl;
		newPage.append(rec);
	}
    newPage.setType(splitMe.getType());

	// cout << "Replacing current pages in upper half" << endl;
	// Replace current page contents with upper half
	splitMe.clear();
    splitMe.setType(newPage.getType());
	// cout << "finished clearing original page" << endl;
	for (auto &rec : upper) {
		// cout << "In upper append" << endl;
		// cout << "Appending in upper half: " << rec << endl;
		splitMe.append(rec);
	}
	
	// Return an internal record (key, ptr) pointing to the new page
	MyDB_INRecordPtr newINRec = getINRecord();
	newINRec->setKey(getKey(lower.back()));
	newINRec->setPtr(newPageNumber);
	return newINRec;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	MyDB_PageReaderWriter currentPage = (*this)[whichPage];
	// cout << "Appending record in lower level append on page " << whichPage << endl;
	// Leaf node just append
	if (currentPage.getType() == MyDB_PageType::RegularPage) {
		// cout << "Adding record to leaf node" << endl;
		bool success = currentPage.append(appendMe);
		if (!success) { // split page
			// cout << "Not successful in adding leaf node" << endl;
			return split(currentPage, appendMe);
		}
	} else if (currentPage.getType() == MyDB_PageType::DirectoryPage) {
		// cout << "In internal page, going to next layer" << endl;
		// internal node, recursively find page
		MyDB_INRecordPtr inRec = getINRecord();
		MyDB_RecordIteratorPtr iter = currentPage.getIterator(inRec);

		int childPtr = -1;
		// cout << "iter->hasNext() " << iter->hasNext() << endl;
		while (iter->hasNext()) {
			iter->getNext();

			auto compare = buildComparator(appendMe, inRec);
			// cout << "Comparing internal rec with append Me" << endl;
			// cout << "Result of compare: " << compare() << endl;
			// cout << "Internal record ptr: " << inRec->getPtr() << endl;
			if (compare()) {
				// If keyToInsert < inRec's key, go to child pointed to by inRec
				childPtr = inRec->getPtr();
				break;
			}
		}
		// cout << "Going to page " << childPtr << endl;
        if (childPtr == -1) {
            cout << "childPtr is at -1" << endl;
            printTree();
            exit(1);
        }
		MyDB_RecordPtr maybeSplit = append(childPtr, appendMe);
		// Handle potential child split
		if (maybeSplit != nullptr) {
			// cout << "Split happened, adding new ptr to current page " << whichPage << endl;
			if (!currentPage.append(maybeSplit)) {
				// cout << "Failed to add new internal node to current page, splitting again" << endl;
				return split(currentPage, maybeSplit);
			}
			MyDB_INRecordPtr lhs = getINRecord();
			MyDB_INRecordPtr rhs = getINRecord();
			auto compare = buildComparator(lhs, rhs);
			currentPage.sortInPlace(compare, lhs, rhs);
		}
	}

	// printTree();
	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    vector<MyDB_PageReaderWriter> curr = {(*this)[rootLocation]};
    vector<MyDB_PageReaderWriter> children = {};
    int level = 1;
    cout << endl;
    cout << "Root: {" << rootLocation << "}" << endl;
    while (!curr.empty()) {
        cout << "Level " << level++ << endl;
        cout << "[";
        for (auto &page : curr) {
            MyDB_INRecordPtr inRec = getINRecord();
            MyDB_RecordPtr rec = getEmptyRecord();
            MyDB_RecordIteratorAltPtr pageIter = page.getIteratorAlt();
            if (page.getType() == MyDB_PageType::RegularPage) {
                cout << " { LEAF : ";
            } else {
                cout << " { INTERNAL : ";
            }

            while (pageIter->advance()) { 
                if (page.getType() == MyDB_PageType::DirectoryPage) {
                    pageIter->getCurrent(inRec);
                    cout << getKey(inRec)->toString() <<  "-" << inRec->getPtr() << " ";
                    children.push_back((*this)[inRec->getPtr()]);
                } else {
                    pageIter->getCurrent(rec);
                    cout << getKey(rec)->toString() << " ";
                }
            }
            cout << " } ";
        }
        cout << "]" << endl;
        curr = children;
        children.clear();
    }
    cout << endl;
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

function<bool()> MyDB_BPlusTreeReaderWriter :: buildEqualToComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the RHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] { return lhAtt->toInt () <= rhAtt->toInt (); };
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] { return lhAtt->toDouble () <= rhAtt->toDouble (); };
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] { return lhAtt->toString () <= rhAtt->toString (); };
	} else {
		cout << "This is bad... cannot do anything with the <=.\n";
		exit (1);
	}
}



#endif
