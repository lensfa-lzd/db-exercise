/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

DiskManager::DiskManager() {
    // memset以字节为单位设置内存 所以要 除sizeof(char)
    memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char)));
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等时 throw InternalError("DiskManager::write_page Error");

    // 计算偏移量, off_t 用于文件偏移量表示的数据类
    off_t off = page_no * PAGE_SIZE;

    // 使用lseek()定位页面地址  将fd定位到 off 位置上
    if (lseek(fd, off, SEEK_SET) == -1) {
        throw InternalError("DiskManager::write_page Error: lseek failed");
    }

    // 使用write()写入数据
    ssize_t bytes_written = write(fd, offset, num_bytes);

    // 检查是否所有数据都被写入
    if (bytes_written != num_bytes) {
        throw InternalError("DiskManager::write_page Error");
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager::read_page Error");

    off_t off = page_no * PAGE_SIZE;
    if (lseek(fd, off, SEEK_SET) == -1) {
        throw InternalError("DiskManager::read_page Error: lseek failed");
    }

    // 使用write()写入数据
    ssize_t bytes_read = read(fd, offset, num_bytes);

    // 检查是否所有数据都被写入
    if (bytes_read != num_bytes) {
        throw InternalError("DiskManager::read_page Error");
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    // stat 结构体用于存储文件或目录的信息，如大小、权限、修改时间等。
    struct stat st{};

    // stat 函数会尝试获取参数中指定的文件或目录的状态信息，并将其存储在 st 中。
    // path.c_str() 是将 std::string 类型的路径转换为 C 风格的字符串，
    // 因为 stat 函数需要 C 风格的字符串作为输入。如果 stat 函数执行成功，它将返回 0。

    // S_ISDIR 是一个宏，用于判断通过 stat 获得的模式（存储在 st.st_mode 中）是否指示这是一个目录。
    // 如果 st.st_mode 表示这是一个目录，S_ISDIR 将返回 true。

    // return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
    // 等价于
    if (stat(path.c_str(), &st) == 0)
        return S_ISDIR(st.st_mode);
    else
        return false;
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true 
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    // struct stat 为结构体
    // stat() 是一个函数
    struct stat st{};
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件

    // 文件权限设置为 rw-r--r--, 其中数字表示法为 0644
    // 权限 0644 表示文件所有者具有读写权限，而组用户和其他用户具有只读权限。

    // O_CREAT 用于创建新文件，
    // O_EXCL 确保如果文件已存在，则 open 调用失败。
    // O_WRONLY 表示文件是为只写打开。
    int fd = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd == -1) {
        // 如果 open 返回 -1, 表示发生错误, 即文件已存在
        throw FileExistsError(path);
    } else {
        // 创建成功，关闭文件
        close(fd);
    }
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // 调用unlink()函数
    // 注意不能删除未关闭的文件

    // 检查文件是否关闭
    if (path2fd_.count(path)){
        // 文件尚未关闭
        throw FileNotClosedError(path);
    }

    // 使用 unlink() 删除文件
    if (unlink(path.c_str()) != 0) {
        throw FileNotFoundError(path);
    }
}


/**
 * @description: 打开指定路径文件 
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表

    // 检查文件是否被创建
    if (!is_file(path)){
        throw FileNotFoundError(path);
    }

    // 检查文件是否已经被打开
    if (path2fd_.count(path)){
        return path2fd_[path];
    }

    // O_RDONLY：只读模式打开。
    // O_WRONLY：只写模式打开。
    // O_RDWR：读写模式打开。
    // O_CREAT：如果文件不存在，则创建它。
    // O_EXCL：与 O_CREAT 一起使用时，如果文件已存在，则返回错误。
    // O_TRUNC：如果文件已存在并且是以写入方式打开的，则其长度被截断至 0。
    // O_APPEND：写操作会在文件的末尾追加数据。
    int fd = open(path.c_str(), O_RDWR);

    // 更新文件打开列表
    path2fd_[path] = fd;
    fd2path_[fd] = path;

    return fd;
}

/**
 * @description:用于关闭指定路径文件 
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表

    // 检查文件
    if (fd2path_.count(fd)){
        if (close(fd) == 0) {
            // 更新文件打开列表
            auto path = fd2path_[fd];
            fd2path_.erase(fd);
            path2fd_.erase(path);
        } else {
            throw InternalError("DiskManager::close_file can't close file");
        }
    }
}


/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf{};
    int rc = stat(file_name.c_str(), &stat_buf);

    // 如果 rc 等于 0（表示 stat 函数成功），则函数返回 stat_buf.st_size，即文件的大小。
    // 如果 rc 不等于 0（表示 stat 函数失败），则函数返回 -1，通常表示无法获取文件大小。
    return rc == 0 ? int(stat_buf.st_size) : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}


/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    return int(bytes_read);
}


/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}