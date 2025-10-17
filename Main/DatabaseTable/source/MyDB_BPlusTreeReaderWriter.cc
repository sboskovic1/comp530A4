
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr lhs, MyDB_AttValPtr rhs) {
    vector<MyDB_PageReaderWriter> pages;
    discoverPages(rootLocation, pages, lhs, rhs);
    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages);
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

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr) {
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter, MyDB_RecordPtr) {
	return nullptr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int, MyDB_RecordPtr) {
	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    vector<MyDB_PageReaderWriter> curr = {(*this)[rootLocation]};
    vector<MyDB_PageReaderWriter> children = {};
    int level = 1;
    while (!curr.empty()) {
        cout << "Level " << level++ << endl;
        cout << "[";
        for (auto &page : curr) {
            
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
