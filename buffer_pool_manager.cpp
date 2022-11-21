#include "buffer/buffer_pool_manager.h"
namespace scudb {
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}
//搜索某一页
/////////////////////////////////////////////////////////////////////////////
//先搜索hash表，如果在hash表中存在，则说明这页在内存中，将其pin值+1，并直接返回。//
//如果不存在，则从空闲链表(首先查找)和LRU置换器中找到一个”旧页“来容纳新页。      //
//如果旧页的脏位为true，则将其写回到到硬盘上。                                 //  
//删除旧页的内容并将新页放入旧页中。                                          //
/////////////////////////////////////////////////////////////////////////////
Page *BufferPoolManager::FetchPage(page_id_t page_id) //
{
  lock_guard<mutex> lck(latch_); //设置互斥，保护数据
  Page *target = nullptr;//指定指针（target)清空
  // 1.1
  if (page_table->Find(page_id,target))
  {
    target->pin_count_++;//存在与哈希表中，将pin值加一
    replacer->Erase(target);
    return target;
  }
  //1.2
  //情况2：不存在于哈希表上的情况
  target = VictimPage();//寻找旧页
  if (target == nullptr)//若都没找到
  return nullptr;//返回空表指针
  //2
  if(target->is_dirty_)//若目标页面(旧页）为脏
  {
    disk_manager_->WritePage(target->GetPageId(),target->data_);//页面数据写回内存
  }
  //3
  page_table_->Remove(target->GetPageId());//删除旧页内容 
  page_table_->Insert(page_id,target);//插入新页
  //4
  disk_manager_->ReadPage(page_id,target->data_);
  target->pin_count_=1;//将新页面pin值设置1
  target->is_dirty_=false;//取消dirty
  target->page_id_=page_id;
  return target;
}
//将某页的pin值减去1，如果该页的pin值小于等于0，则将这页重新放到LRU置换器中去。
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) 
{  
  lock_guard<mutex> lck(latch_);//设置互斥
  Page *target = nullptr;//清空指针
  page_table_->Find(page_id,target);//寻找页面
  if (target == nullptr) {
    return false;
  }
  target->is_dirty_ = is_dirty;//设置页面脏标识符
  if (target->GetPinCount() <= 0)//若pincount小于0
  {
    return false;//返回
  }
  ;
  if (--target->pin_count_ == 0) //若前面的页表pincount等于0
  {
    replacer_->Insert(target);//在这里插入
  }
  return true;//返回成功
}
//将某一页回写到内存
//如果该页的脏位为true，则将其回写到内存中。
bool BufferPoolManager::FlushPage(page_id_t page_id) 
{ 
  lock_guard<mutex> lck(latch_);//设置互斥
  Page *target = nullptr;//清空指针
  page_table_->Find(page_id,target);
  if (target == nullptr || target->page_id_ == INVALID_PAGE_ID) 
  {
    return false;
  }
  if (target->is_dirty_)//如果该页的脏位为true
  {
    disk_manager_->WritePage(page_id,target->GetData());//则将其回写到内存中
    target->is_dirty_ = false;//取消标识符
  }
  return true;//返回
}
//如果该页的pin值小于等于0， 将该页从置换器，hash表中删除，并将其放到空闲链表中。
bool BufferPoolManager::DeletePage(page_id_t page_id) 
{ 
  lock_guard<mutex> lck(latch_);//设置互斥
  Page *target = nullptr;//清空指针
  page_table_->Find(page_id,target);
  if (target != nullptr);
  {
    if (target->GetPinCount() > 0)//若pin值>0，返回（不能删）
    {
      return false;
    }
    //将该页从置换器，hash表中删除，并将其放到空闲链表中
    replacer_->Erase(target);
    page_table_->Remove(page_id);
    target->is_dirty_= false;
    target->ResetMemory();
    free_list_->push_back(target);
    ///////////////////////////////////////////////
  }
  disk_manager_->DeallocatePage(page_id);
  return true;
}
//从空闲链表(首先查找)和LRU置换器中找到一个”旧页“来容纳新页。
//如果旧页的脏位为true，则将其写回到到硬盘上。
//删除旧页的内容并将新页放入旧页中。
Page *BufferPoolManager::NewPage(page_id_t &page_id) 
{ 
  lock_guard<mutex> lck(latch_);//设置互斥
  Page *target = nullptr;//清空指针
  target = GetVictimPage();
  if (target == nullptr) 
  return target;
  page_id = disk_manager_->AllocatePage();
  if (target->is_dirty_) //如果旧页的脏位为true
  {
    disk_manager_->WritePage(tar->GetPageId(),target->data_);//则将其写回到到硬盘上
  }
  page_table_->Remove(target->GetPageId());//删除旧页的内容
  page_table_->Insert(page_id,target);//将新页放入旧页中
  //清空内存
  target->page_id_ = page_id;
  target->ResetMemory();
  target->is_dirty_ = false;
  target->pin_count_ = 1;
  return target;
}
//新增函数--找出合适页面
Page *BufferPoolManager::VictimPage() {
  Page *target = nullptr;
  if (free_list_->empty()) //若空闲链表为空
  {
    if (replacer_->Size() == 0) //且置换器没冗余
    {
      return nullptr;//寻找失败，返回空指针
    }
    replacer_->Victim(target);
  } 
  else 
  {
    target = free_list_->front();
    free_list_->pop_front();
    assert(target->GetPageId() == INVALID_PAGE_ID);
  }
  assert(target->GetPinCount() == 0);
  return target;
}
} // namespace scudb
