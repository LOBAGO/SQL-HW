/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt = 0;

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *target = FindLeafPage(key,false,Operation::READ,transaction);
  if (target == nullptr)
    return false;
  
  result.resize(1);
  auto ret = target->Lookup(key,result[0],comparator_);
  
  FreePagesInTransaction(false,transaction,target->GetPageId());
 
  return ret;
}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  LockRootPageId(true);
  if (IsEmpty()) {
    StartNewTree(key,value);
    TryUnlockRootPageId(true);
    return true;
  }
  TryUnlockRootPageId(true);
  bool res = InsertIntoLeaf(key,value,transaction);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t newPageId;
  Page *rootPage = buffer_pool_manager_->NewPage(newPageId);
  assert(rootPage != nullptr);

  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootPage->GetData());

  
  root->Init(newPageId,INVALID_PAGE_ID);
  root_page_id_ = newPageId;
  UpdateRootPageId(true);
 
  root->Insert(key,value,comparator_);

  buffer_pool_manager_->UnpinPage(newPageId,true);
}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = FindLeafPage(key,false,Operation::INSERT,transaction);
  ValueType v;
  bool exist = leafPage->Lookup(key,v,comparator_);
  if (exist) {
    
    FreePagesInTransaction(true,transaction);
    return false;
  }
  leafPage->Insert(key,value,comparator_);
  if (leafPage->GetSize() > leafPage->GetMaxSize()) {//insert then split
    B_PLUS_TREE_LEAF_PAGE_TYPE *newLeafPage = Split(leafPage,transaction);//unpin it in below func
    InsertIntoParent(leafPage,newLeafPage->KeyAt(0),newLeafPage,transaction);
  }
  
  FreePagesInTransaction(true,transaction);
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  page_id_t newPageId;
  Page* const newPage = buffer_pool_manager_->NewPage(newPageId);
  assert(newPage != nullptr);
  newPage->WLatch();
  transaction->AddIntoPageSet(newPage);
  
  N *newNode = reinterpret_cast<N *>(newPage->GetData());
  newNode->Init(newPageId, node->GetParentPageId());
  node->MoveHalfTo(newNode, buffer_pool_manager_);
 
  return newNode;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page* const newPage = buffer_pool_manager_->NewPage(root_page_id_);
    assert(newPage != nullptr);
    assert(newPage->GetPinCount() == 1);
    B_PLUS_TREE_INTERNAL_PAGE *newRoot = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(newPage->GetData());
    newRoot->Init(root_page_id_);
    newRoot->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
   
    buffer_pool_manager_->UnpinPage(newRoot->GetPageId(),true);
    return;
  }
  page_id_t parentId = old_node->GetParentPageId();
  auto *page = FetchPage(parentId);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
  new_node->SetParentPageId(parentId);
  
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (parent->GetSize() > parent->GetMaxSize()) {
   
    B_PLUS_TREE_INTERNAL_PAGE *newLeafPage = Split(parent,transaction);
    InsertIntoParent(parent,newLeafPage->KeyAt(0),newLeafPage,transaction);
  }
  buffer_pool_manager_->UnpinPage(parentId,true);
                                      
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  B_PLUS_TREE_LEAF_PAGE_TYPE *delTar = FindLeafPage(key,false,Operation::DELETE,transaction);
  int curSize = delTar->RemoveAndDeleteRecord(key,comparator_);
  if (curSize < delTar->GetMinSize()) {
    CoalesceOrRedistribute(delTar,transaction);
  }
  FreePagesInTransaction(true,transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    bool delOldRoot = AdjustRoot(node);
    if (delOldRoot) {transaction->AddIntoDeletedPageSet(node->GetPageId());}
    return delOldRoot;
  }
  
  N *node2;
  bool isRightSib = FindLeftSibling(node,node2,transaction);
  BPlusTreePage *parent = FetchPage(node->GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parentPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent);
  
  if (node->GetSize() + node2->GetSize() <= node->GetMaxSize()) {
    if (isRightSib) {swap(node,node2);} 
    int removeIndex = parentPage->ValueIndex(node->GetPageId());
    Coalesce(node2,node,parentPage,removeIndex,transaction);
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    return true;
  }
  
  int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
  Redistribute(node2,node,nodeInParentIndex);
  buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindLeftSibling(N *node, N * &sibling, Transaction *transaction) {
  auto page = FetchPage(node->GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
  int index = parent->ValueIndex(node->GetPageId());
  int siblingIndex = index - 1;
  if (index == 0) { 
    siblingIndex = index + 1;
  }
  sibling = reinterpret_cast<N *>(CrabingProtocalFetchPage(
          parent->ValueAt(siblingIndex),Operation::DELETE,-1,transaction));
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  return index == 0;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
  
  node->MoveAllTo(neighbor_node,index,buffer_pool_manager_);
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  parent->Remove(index);
  if (parent->GetSize() <= parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent,transaction);
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node,buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    assert(old_root_node->GetSize() == 0);
    assert (old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  if (old_root_node->GetSize() == 1) {
    B_PLUS_TREE_INTERNAL_PAGE *root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
    const page_id_t newRootId = root->RemoveAndReturnOnlyChild();
    root_page_id_ = newRootId;
    UpdateRootPageId();
    
    Page *page = buffer_pool_manager_->FetchPage(newRootId);
    assert(page != nullptr);
    B_PLUS_TREE_INTERNAL_PAGE *newRoot =
            reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
    newRoot->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(newRootId, true);
    return true;
  }
  return false;
}


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType useless;
  auto start_leaf = FindLeafPage(useless, true);
  TryUnlockRootPageId(false);
  return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto start_leaf = FindLeafPage(key);
  TryUnlockRootPageId(false);
  if (start_leaf == nullptr) {
    return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
  }
  int idx = start_leaf->KeyIndex(key,comparator_);
  return INDEXITERATOR_TYPE(start_leaf, idx, buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost,
                                                         Operation op,
                                                         Transaction *transaction) {
  bool exclusive = (op != Operation::READ);
  LockRootPageId(exclusive);
  if (IsEmpty()) {
    TryUnlockRootPageId(exclusive);
    return nullptr;
  }
  
  auto pointer = CrabingProtocalFetchPage(root_page_id_,op,-1,transaction);
  page_id_t next;
  for (page_id_t cur = root_page_id_; !pointer->IsLeafPage(); pointer =
          CrabingProtocalFetchPage(next,op,cur,transaction),cur = next) {
    B_PLUS_TREE_INTERNAL_PAGE *internalPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pointer);
    if (leftMost) {
      next = internalPage->ValueAt(0);
    }else {
      next = internalPage->Lookup(key,comparator_);
    }
  }
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pointer);
}
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id,Operation op,page_id_t previous, Transaction *transaction) {
  bool exclusive = op != Operation::READ;
  auto page = buffer_pool_manager_->FetchPage(page_id);
  Lock(exclusive,page);
  auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (previous > 0 && (!exclusive || treePage->IsSafe(op))) {
    FreePagesInTransaction(exclusive,transaction,previous);
  }
  if (transaction != nullptr)
    transaction->AddIntoPageSet(page);
  return treePage;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t cur) {
  TryUnlockRootPageId(exclusive);
  if (transaction == nullptr) {
    assert(!exclusive && cur >= 0);
    Unlock(false,cur);
    buffer_pool_manager_->UnpinPage(cur,false);
    return;
  }
  for (Page *page : *transaction->GetPageSet()) {
    int curPid = page->GetPageId();
    Unlock(exclusive,page);
    buffer_pool_manager_->UnpinPage(curPid,exclusive);
    if (transaction->GetDeletedPageSet()->find(curPid) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(curPid);
      transaction->GetDeletedPageSet()->erase(curPid);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  if (IsEmpty()) {
    return "Empty tree";
  }
  std::queue<BPlusTreePage *> todo, tmp;
  std::stringstream tree;
  auto node = reinterpret_cast<BPlusTreePage *>(
          buffer_pool_manager_->FetchPage(root_page_id_));
  if (node == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
  }
  todo.push(node);
  bool first = true;
  while (!todo.empty()) {
    node = todo.front();
    if (first) {
      first = false;
      tree << "| ";
    }
    
    if (node->IsLeafPage()) {
      auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
      tree << page->ToString(verbose) <<"("<<node->GetPageId()<< ")| ";
    } else {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
      tree << page->ToString(verbose) <<"("<<node->GetPageId()<< ")| ";
      page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty()) {
      todo.swap(tmp);
      tree << '\n';
      first = true;
    }
    
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return tree.str();
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}


INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::isBalanced(page_id_t pid) {
  if (IsEmpty()) return true;
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,"all page are pinned while isBalanced");
  }
  int ret = 0;
  if (!node->IsLeafPage())  {
    auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
    int last = -2;
    for (int i = 0; i < page->GetSize(); i++) {
      int cur = isBalanced(page->ValueAt(i));
      if (cur >= 0 && last == -2) {
        last = cur;
        ret = last + 1;
      }else if (last != cur) {
        ret = -1;
        break;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(pid,false);
  return ret;
}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid,pair<KeyType,KeyType> &out) {
  if (IsEmpty()) return true;
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,"all page are pinned while isPageCorr");
  }
  bool ret = true;
  if (node->IsLeafPage())  {
    auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    int size = page->GetSize();
    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    for (int i = 1; i < size; i++) {
      if (comparator_(page->KeyAt(i-1), page->KeyAt(i)) > 0) {
        ret = false;
        break;
      }
    }
    out = pair<KeyType,KeyType>{page->KeyAt(0),page->KeyAt(size-1)};
  } else {
    auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    int size = page->GetSize();
    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    pair<KeyType,KeyType> left,right;
    for (int i = 1; i < size; i++) {
      if (i == 1) {
        ret = ret && isPageCorr(page->ValueAt(0),left);
      }
      ret = ret && isPageCorr(page->ValueAt(i),right);
      ret = ret && (comparator_(page->KeyAt(i) ,left.second)>0 && comparator_(page->KeyAt(i), right.first)<=0);
      ret = ret && (i == 1 || comparator_(page->KeyAt(i-1) , page->KeyAt(i)) < 0);
      if (!ret) break;
      left = right;
    }
    out = pair<KeyType,KeyType>{page->KeyAt(0),page->KeyAt(size-1)};
  }
  buffer_pool_manager_->UnpinPage(pid,false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check(bool forceCheck) {
  if (!forceCheck && !openCheck) {
    return true;
  }
  pair<KeyType,KeyType> in;
  bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
  bool isBal = (isBalanced(root_page_id_) >= 0);
  bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();
  if (!isPageInOrderAndSizeCorr) cout<<"problem in page order or page size"<<endl;
  if (!isBal) cout<<"problem in balance"<<endl;
  if (!isAllUnpin) cout<<"problem in page unpin"<<endl;
  return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} 