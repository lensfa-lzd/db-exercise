/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::scoped_lock lock{latch_};  //  如果编译报错可以替换成其他lock

    // 检查 LRUlist_ 是否为空
    if (LRUlist_.empty()) {
        // 如果列表为空，则没有可淘汰的页面
        frame_id = nullptr;
        return false;
    }

    // 从 LRUlist_ 的末尾获取最久未使用的 frame id
    frame_id_t victim_id = LRUlist_.back();
    LRUlist_.pop_back();  // 从列表中移除该元素

    // 从 LRUhash_ 中移除该 frame id 的映射
    LRUhash_.erase(victim_id);

    // 将 victim_id 的值传递给调用者
    *frame_id = victim_id;

    // 返回时自动解锁
    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    // 固定指定id的frame
    // 在数据结构中移除该frame

    // 先要确认 frame_id 是否存在于缓冲区
    if (LRUhash_.count(frame_id)) {
        auto frame = LRUhash_[frame_id];
        LRUlist_.erase(frame);
        LRUhash_.erase(frame_id);
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};  // 保证线程安全

    // 查找 frame_id 是否已经存在与缓冲区之中
    if (!LRUhash_.count(frame_id)) {
        // 将该 frame_id 插入到 LRUlist_ 的首部
        LRUlist_.emplace_front(frame_id);
        // LRUlist_.end() 实际为队尾再后面的一个元素
        LRUhash_[frame_id] = LRUlist_.begin();
    }
    // 忽略重复的操作
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
