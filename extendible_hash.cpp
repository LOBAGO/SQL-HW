#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {


template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :  globalDepth(0),bucketSize(size),bucketNum(1) 
{
  buckets.push_back(make_shared<Bucket>(0));
}
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash()//构造哈希表 
{
  ExtendibleHash(64);
}
template <typename K, typename V>
//计算哈希K值
size_t ExtendibleHash<K, V>::HashKey(const K &key) 
{
  return hash<K>{}(key);
}
//获取全局深度
template <typename K, typename V>
int ExtendibleHash<K, V>::GlobalDepth() const 
{
  lock_guard<mutex> lock(latch);//保护数据
  return globalDepth;
}
//获取局部深度
template <typename K, typename V>
int ExtendibleHash<K, V>::LocalDepth(int bucket_id) const 
{
  if (buckets[bucket_id]) 
  {
    lock_guard<mutex> lck(buckets[bucket_id]->latch);//保护桶数据
    if (buckets[bucket_id]->kmap.size() == 0)//深度=0 
    return -1;//获取失败
    return buckets[bucket_id]->localDepth;
  }
  return -1;
}
//获取桶数字
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const 
{
  lock_guard<mutex> lock(latch);//设置互斥
  return bucketNum;
}
//查找数据
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) 
{
  int index = getIdx(key);
  lock_guard<mutex> lck(buckets[index]->latch);//保护数据
  if (buckets[index]->kmap.find(key) != buckets[index]->kmap.end()) 
  {
    value = buckets[index]->kmap[key];
    return true;
  }
  return false;
}
//
template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K &key) const
{
  lock_guard<mutex> lck(latch);
  return HashKey(key) & ((1 << globalDepth) - 1);
}

//删除数据
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) 
{
  int index = getIdx(key);
  lock_guard<mutex> lck(buckets[index]->latch);//保护数据
  shared_ptr<Bucket> temp = buckets[index];
  if (cur->kmap.find(key) == cur->kmap.end()) 
  {
    return false;
  }
  temp->kmap.erase(key);
  return true;
}
//插入
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) 
{
  int index = getIdx(key);
  shared_ptr<Bucket> temp = buckets[index];
  while (true) {
    lock_guard<mutex> lck(temp->latch);
    if (temp->kmap.find(key) != temp->kmap.end() || temp->kmap.size() < bucketSize) 
    {
      temp->kmap[key] = value;
      break;
    }
    int mask = (1 << (temp->localDepth));
    temp->localDepth++;

    {
      lock_guard<mutex> lck2(latch);//保护数据
      if (temp->localDepth > globalDepth) {

        size_t L = buckets.size();
        for (size_t i = 0; i < L; i++) {
          buckets.push_back(buckets[i]);
        }
        globalDepth++;
      }
      bucketNum++;
      auto newBucket = make_shared<Bucket>(temp->localDepth);
      typename map<K, V>::iterator it;
      for (it = temp->kmap.begin(); it != temp->kmap.end(); ) {
        if (HashKey(it->first) & mask) {
          newBucket->kmap[it->first] = it->second;
          it = temp->kmap.erase(it);
        } else it++;
      }
      for (size_t i = 0; i < buckets.size(); i++) {
        if (buckets[i] == cur && (i & mask))
          buckets[i] = newBucket;
      }
    }
    index = getIdx(key);
    temp = buckets[index];
  }
}
template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
