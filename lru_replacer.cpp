/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {
//数据结构
template <typename T> LRUReplacer<T>::LRUReplacer() 
{
  head = make_shared<Node>();
  tail = make_shared<Node>();
  head->next = tail;
  tail->prev = head;
}
//函数重构
template <typename T> LRUReplacer<T>::~LRUReplacer() 
{}
typename T> void LRUReplacer<T>::Insert(const T &value) 
{
  lock_guard<mutex> lck(latch);//保护数据
  shared_ptr<Node> temp;
  if (map.find(value) != map.end()) 
  {
    temp = map[value];
    shared_ptr<Node> prev = temp->prev;
    shared_ptr<Node> succ = temp->next;
    prev->next = succ;
    succ->prev = prev;
  } 
  else 
  {
    temp = make_shared<Node>(value);
  }
  shared_ptr<Node> fir = head->next;
  temp->next = fir;
  fir->prev = temp;
  temp->prev = head;
  head->next = temp;
  map[value] = temp;
  return;
}
template <typename T> bool LRUReplacer<T>::Victim(T &value) 
{
  lock_guard<mutex> lck(latch);//保护数据
  if (map.empty()) 
  {
    return false;
  }
  shared_ptr<Node> last = tail->prev;
  tail->prev = last->prev;
  last->prev->next = tail;
  value = last->val;
  map.erase(last->val);
  return true;
}

template <typename T> bool LRUReplacer<T>::Erase(const T &value) 
{
    lock_guard<mutex> lck(latch);//保护数据
  if (map.find(value) != map.end()) 
  {
    shared_ptr<Node> temp = map[value];
    temp->prev->next = temp->next;
    temp->next->prev = temp->prev;
  }
  return map.erase(value);
}
template <typename T> size_t LRUReplacer<T>::Size() 
{ 
  lock_guard<mutex> lck(latch);//互斥保护
  return map.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
