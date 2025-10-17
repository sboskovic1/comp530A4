
#ifndef BPLUS_C
#define BPLUS_C

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
	rootLocation = getTable ()->getRootLocation ();
	cout << "Initiating bplus tree with root location: " << rootLocation << endl;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr lhs, MyDB_AttValPtr rhs) {
    vector<MyDB_PageReaderWriter> pages;
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
    vector<MyDB_PageReaderWriter> pages;
    discoverPages(rootLocation, pages, lhs, rhs);
    return make_shared<MyDB_PageListIteratorAlt>(pages);
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> & list, MyDB_AttValPtr lhs, MyDB_AttValPtr rhs) {
    MyDB_RecordPtr currentRec = make_shared<MyDB_Record>(nullptr);
    MyDB_PageReaderWriter page = (*this)[whichPage];
    if (page.getType() != MyDB_PageType::RegularPage) {
        list.push_back(page);
        return true;
    }
    MyDB_RecordIteratorAltPtr iter = page.getIteratorAlt();
    bool lastPage = false;
    while (iter->advance() && !lastPage) {
        iter->getCurrent(currentRec);
        MyDB_AttValPtr key = getKey(currentRec);
        auto leftCmp = buildComparator(currentRec, make_shared<MyDB_Record>(MyDB_INRecord(lhs)));
        auto rightCmp = buildComparator(make_shared<MyDB_Record>(MyDB_INRecord(rhs)), currentRec);
        if (leftCmp() && !lastPage) { // Use custom comparator to check if this page should be added
            int idx = static_pointer_cast<MyDB_INRecord>(currentRec)->getPtr();
            if (!discoverPages(idx, list, lhs, rhs) && rightCmp()) {
                lastPage = true;
            }
        }
    }
	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
	cout << "appending record in high level append " << appendMe << endl;
	// Base case for an empty tree: Create a internal node (root) with an infinity internal record 
	// that points to an empty leaf page
	if (rootLocation == -1) {
		cout << "Creating initial root node for b plus tree" << endl;
		rootLocation = this->getNumPages();

		// Create internal node with infinity internal record
		cout << "Creating initial root page" << endl;
		MyDB_PageReaderWriter rootPage = MyDB_PageReaderWriter(*this, rootLocation);
		rootPage.setType(MyDB_PageType::DirectoryPage);

		cout << "Creating initial internal record with inifnity key" << endl;
		MyDB_INRecord newINRec = MyDB_INRecord(getKey(appendMe));

		// Point root node to an empty leaf page
		int newPageNumber = this->getNumPages();
		MyDB_PageReaderWriter newPage = MyDB_PageReaderWriter(*this, newPageNumber);
		newPage.setType(MyDB_PageType::RegularPage);
		newINRec.setPtr(newPageNumber);
		
		cout << "Adding initial internal record to rootPage" << endl;
		rootPage.append(make_shared<MyDB_INRecord>(newINRec));
		cout << "Done adding initial internal record to rootPage" << endl;
	}


	// Create new internal page if root gets split
	MyDB_RecordPtr maybeSplit = append(rootLocation, appendMe);
	if (maybeSplit != nullptr) {
		int newPageNumber = this->getNumPages();
		MyDB_PageReaderWriter newPage = MyDB_PageReaderWriter(*this, newPageNumber);
		newPage.setType(MyDB_PageType::DirectoryPage);
		newPage.append(maybeSplit);
		
		// Add a new internal node to point to the old root (key is automatically to largest possible value)
		MyDB_INRecord newINRec = MyDB_INRecord(getKey(appendMe));
		newINRec.setPtr(rootLocation);
		newPage.append(make_shared<MyDB_INRecord>(newINRec));
		rootLocation = newPageNumber;
		getTable()->setRootLocation(newPageNumber);
	}
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
	// Sort all records (splitMe records + andMe)
	vector<MyDB_RecordPtr> records;
	MyDB_RecordPtr tempRec = getEmptyRecord();
	MyDB_RecordIteratorPtr iter = splitMe.getIterator(tempRec);

	while (iter->hasNext()) {
		iter->getNext();

		// Deep copy of the record
		// Allocate a temporary buffer for copying the record
		size_t size = tempRec->getBinarySize();
		vector<char> buffer(size);

		// Serialize the other record into the buffer
		tempRec->toBinary(buffer.data());

		// Copy the binary into a new record
		MyDB_RecordPtr recordCopy = getEmptyRecord();
		recordCopy->fromBinary(buffer.data());

		records.push_back(recordCopy);
	}
	records.push_back(andMe);

	std::sort(records.begin(), records.end(), [this](MyDB_RecordPtr a, MyDB_RecordPtr b) {
		auto compare = buildComparator(a, b);
		return compare();
	});

	// Split records in half
	int mid = records.size() / 2;
	vector<MyDB_RecordPtr> lower(records.begin(), records.begin() + mid);
	vector<MyDB_RecordPtr> upper(records.begin() + mid, records.end());

	// Create a new page for the lower half
	int newPageNumber = this->getNumPages();
	MyDB_PageReaderWriter newPage = MyDB_PageReaderWriter(*this, newPageNumber);
	newPage.setType(splitMe.getType());
	for (auto &rec : lower) {
		newPage.append(rec);
	}

	// Replace current page contents with upper half
	splitMe.clear();
	for (auto &rec : upper) {
		splitMe.append(rec);
	}

	// Return an internal record (key, ptr) pointing to the new page
	MyDB_INRecord newINRec = MyDB_INRecord(getKey(lower.back()));
	newINRec.setPtr(newPageNumber);
	return make_shared<MyDB_INRecord>(newINRec);
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	cout << "Appending record in lower level append on page " << whichPage << endl;
	MyDB_PageReaderWriter currentPage = (*this)[whichPage];

	// Leaf node just append
	if (currentPage.getType() == MyDB_PageType::RegularPage) {
		bool success = currentPage.append(appendMe);
		if (!success) { // split page
			return split(currentPage, appendMe);
		}
	} else if (currentPage.getType() == MyDB_PageType::DirectoryPage) {
		// internal node, recursively find page
		MyDB_INRecordPtr inRec = getINRecord();
		MyDB_RecordIteratorPtr iter = currentPage.getIterator(inRec);

		int childPtr = -1;
		while (iter->hasNext()) {
			iter->getNext();

			auto compare = buildComparator(inRec, appendMe);
			if (compare()) {
				// If keyToInsert < inRec's key, go to child pointed to by inRec
				childPtr = inRec->getPtr();
				break;
			}
		}

		MyDB_RecordPtr maybeSplit = append(childPtr, appendMe);
		// Handle potential child split
		if (maybeSplit != nullptr) {
			if (!currentPage.append(maybeSplit)) {
				return split(currentPage, maybeSplit);
			}
			MyDB_RecordPtr lhs = getEmptyRecord();
			MyDB_RecordPtr rhs = getEmptyRecord();
			auto compare = buildComparator(lhs, rhs);
			currentPage.sort(compare, lhs, rhs);
		}
	}

	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    vector<MyDB_PageReaderWriter> curr = {(*this)[rootLocation]};
    vector<MyDB_PageReaderWriter> children = {};
    int level = 1;
    cout << "Root: {" << rootLocation << "}" << endl;
    while (!curr.empty()) {
        cout << "Level " << level++ << endl;
        cout << "[";
        for (auto &page : curr) {
			MyDB_RecordPtr temp = getEmptyRecord();
			MyDB_RecordIteratorAltPtr pageIter = page.getIteratorAlt();
			while (pageIter->advance()) {
				pageIter->getCurrent(temp);
                MyDB_AttValPtr key = getKey(temp);
                cout << "{ " << (page.getType() == MyDB_PageType::RegularPage ? "INTERNAL" : "LEAF") << " : ";
                cout << key->toString() << " }";
				if (page.getType() == MyDB_PageType::DirectoryPage) {
					children.push_back(page);
				}
			}
        }
        cout << "]" << endl;
        curr = children;
        children.clear();
    }
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


#endif
