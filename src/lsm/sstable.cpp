#include "lsm/sstable.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>

#include "common/coding.h"
#include "common/crc32c.h"
#include "lsm/bloom_filter.h"

namespace kv::lsm {

namespace {

constexpr std::size_t kWriteBufferBytes = 256 * 1024;

void putBytes(std::string& out, std::string_view s) {
    putFixed32(out, static_cast<std::uint32_t>(s.size()));
    out.append(s);
}

// Appends `payload` followed by its CRC-32C.
std::string withCrc(std::string payload) {
    putFixed32(payload, crc32c(payload));
    return payload;
}

// Checks and removes the trailing CRC-32C. Returns false if it doesn't match.
bool checkCrc(std::string_view framed, std::string_view& payload) {
    if (framed.size() < 4) return false;
    payload = framed.substr(0, framed.size() - 4);
    return decodeFixed32(framed.data() + payload.size()) == crc32c(payload);
}

[[noreturn]] void corrupt(const std::string& path, const std::string& what) {
    throw CorruptTableError(path + ": " + what);
}

}  // namespace

// ---------------------------------------------------------------------------
// SstWriter
// ---------------------------------------------------------------------------

SstWriter::SstWriter(const std::string& path, std::size_t blockBytes, int bloomBitsPerKey, bool sync)
    : path_(path), blockBytes_(blockBytes), bloomBitsPerKey_(bloomBitsPerKey), sync_(sync) {
    fd_ = storage::openOrThrow(path, O_WRONLY | O_CREAT | O_TRUNC);
}

SstWriter::~SstWriter() = default;

void SstWriter::add(std::string_view key, bool tombstone, std::string_view value) {
    if (finished_) throw std::logic_error("sstable: add() after finish()");
    if (entries_ > 0 && key <= lastKey_) throw std::logic_error("sstable: keys must be added in increasing order");

    if (entries_ == 0) firstKey_ = key;
    lastKey_ = key;
    keys_.emplace_back(key);
    ++entries_;

    block_ += static_cast<char>(tombstone ? 0 : 1);
    putBytes(block_, key);
    putBytes(block_, tombstone ? std::string_view() : value);

    if (block_.size() >= blockBytes_) flushBlock();
}

void SstWriter::append(std::string_view bytes) {
    buffer_.append(bytes);
    if (buffer_.size() >= kWriteBufferBytes) flushBuffer();
}

void SstWriter::flushBuffer() {
    const char* p = buffer_.data();
    std::size_t left = buffer_.size();
    while (left > 0) {  // write() may write fewer bytes than asked
        ssize_t n = ::write(fd_.get(), p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            storage::throwErrno("write " + path_);
        }
        p += n;
        left -= static_cast<std::size_t>(n);
    }
    fileBytes_ += buffer_.size();
    buffer_.clear();
}

// Seals the data block under construction: add its CRC, remember where it is in the index.
void SstWriter::flushBlock() {
    if (block_.empty()) return;
    std::string framed = withCrc(std::move(block_));
    block_.clear();

    index_.push_back(IndexLine{lastKey_, fileBytes_ + buffer_.size(), static_cast<std::uint32_t>(framed.size())});
    append(framed);
}

TableInfo SstWriter::finish() {
    if (entries_ == 0) throw std::logic_error("sstable: refusing to write an empty table");
    flushBlock();

    const std::uint64_t filterOffset = fileBytes_ + buffer_.size();
    const std::string filter = withCrc(BloomFilter::build(keys_, bloomBitsPerKey_));
    append(filter);

    const std::uint64_t indexOffset = fileBytes_ + buffer_.size();
    std::string index;
    putFixed32(index, static_cast<std::uint32_t>(index_.size()));
    putFixed64(index, entries_);
    putBytes(index, firstKey_);
    putBytes(index, lastKey_);
    for (const IndexLine& line : index_) {
        putBytes(index, line.lastKey);
        putFixed64(index, line.offset);
        putFixed32(index, line.size);
    }
    index = withCrc(std::move(index));
    append(index);

    std::string footer;
    putFixed64(footer, filterOffset);
    putFixed64(footer, filter.size());
    putFixed64(footer, indexOffset);
    putFixed64(footer, index.size());
    putFixed64(footer, entries_);
    putFixed64(footer, kSstMagic);
    putFixed32(footer, crc32c(footer));
    append(footer);

    flushBuffer();
    if (sync_ && ::fsync(fd_.get()) != 0) storage::throwErrno("fsync " + path_);
    fd_ = storage::UniqueFd();  // close
    finished_ = true;

    TableInfo info;
    info.fileSize = fileBytes_;
    info.entries = entries_;
    info.smallest = firstKey_;
    info.largest = lastKey_;
    return info;
}

// ---------------------------------------------------------------------------
// SstReader
// ---------------------------------------------------------------------------

std::string SstReader::readAt(std::uint64_t offset, std::uint64_t size) const {
    std::string buf(size, '\0');
    std::size_t done = 0;
    while (done < size) {
        ssize_t n = ::pread(fd_.get(), buf.data() + done, size - done, static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) continue;
            storage::throwErrno("read " + path_);
        }
        if (n == 0) corrupt(path_, "unexpected end of file");
        done += static_cast<std::size_t>(n);
    }
    return buf;
}

std::shared_ptr<SstReader> SstReader::open(const std::string& path) {
    std::shared_ptr<SstReader> r(new SstReader());
    r->path_ = path;
    r->fd_ = storage::openOrThrow(path, O_RDONLY);

    off_t size = ::lseek(r->fd_.get(), 0, SEEK_END);
    if (size < static_cast<off_t>(kFooterSize)) corrupt(path, "too small to be a table");
    r->fileSize_ = static_cast<std::uint64_t>(size);

    // The footer, always at the end, says where everything else is.
    const std::string footer = r->readAt(r->fileSize_ - kFooterSize, kFooterSize);
    std::string_view footerBody;
    if (!checkCrc(footer, footerBody)) corrupt(path, "bad footer checksum");
    ByteReader f(footerBody);
    std::uint64_t filterOffset, filterSize, indexOffset, indexSize, entries, magic;
    if (!f.getFixed64(filterOffset) || !f.getFixed64(filterSize) || !f.getFixed64(indexOffset) ||
        !f.getFixed64(indexSize) || !f.getFixed64(entries) || !f.getFixed64(magic) || magic != kSstMagic)
        corrupt(path, "not a table (bad magic number)");
    const std::uint64_t dataEnd = r->fileSize_ - kFooterSize;
    if (filterOffset + filterSize > dataEnd || indexOffset + indexSize > dataEnd || filterSize < 4 || indexSize < 4)
        corrupt(path, "footer points outside the file");
    r->entries_ = entries;

    std::string_view body;
    const std::string filter = r->readAt(filterOffset, filterSize);
    if (!checkCrc(filter, body)) corrupt(path, "bad filter checksum");
    r->filter_ = std::string(body);

    const std::string index = r->readAt(indexOffset, indexSize);
    if (!checkCrc(index, body)) corrupt(path, "bad index checksum");
    ByteReader in(body);
    std::uint32_t blocks;
    std::uint64_t indexedEntries;
    if (!in.getFixed32(blocks) || !in.getFixed64(indexedEntries) || indexedEntries != entries ||
        !in.getLengthPrefixed(~0u, r->smallest_) || !in.getLengthPrefixed(~0u, r->largest_) || blocks == 0 ||
        blocks > in.remaining() / 16)
        corrupt(path, "malformed index");
    for (std::uint32_t i = 0; i < blocks; ++i) {
        IndexLine line;
        if (!in.getLengthPrefixed(~0u, line.lastKey) || !in.getFixed64(line.offset) || !in.getFixed32(line.size) ||
            line.size < 4 || line.offset + line.size > filterOffset)
            corrupt(path, "malformed index entry");
        if (!r->index_.empty() && r->index_.back().lastKey >= line.lastKey) corrupt(path, "index is not sorted");
        r->index_.push_back(std::move(line));
    }
    if (!in.atEnd()) corrupt(path, "trailing bytes in the index");
    return r;
}

void SstReader::readBlock(std::size_t block, std::vector<Iterator::Item>& out) const {
    const IndexLine& line = index_.at(block);
    const std::string framed = readAt(line.offset, line.size);
    std::string_view body;
    if (!checkCrc(framed, body)) corrupt(path_, "bad checksum in data block " + std::to_string(block));

    out.clear();
    ByteReader in(body);
    while (!in.atEnd()) {
        std::uint8_t flag;
        Iterator::Item item;
        std::string value;
        if (!in.getU8(flag) || flag > 1 || !in.getLengthPrefixed(~0u, item.key) || !in.getLengthPrefixed(~0u, value))
            corrupt(path_, "malformed entry in data block " + std::to_string(block));
        item.tombstone = flag == 0;
        item.value = std::move(value);
        if (!out.empty() && out.back().key >= item.key) corrupt(path_, "keys out of order in data block");
        out.push_back(std::move(item));
    }
    if (out.empty() || out.back().key != line.lastKey) corrupt(path_, "data block does not match the index");
}

std::size_t SstReader::blockFor(std::string_view key) const {
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
                               [](const IndexLine& line, std::string_view k) { return line.lastKey < k; });
    return static_cast<std::size_t>(it - index_.begin());
}

std::optional<LookupResult> SstReader::get(std::string_view key) const {
    if (key < smallest_ || key > largest_) return std::nullopt;
    if (!BloomFilter::mayContain(filter_, key)) return std::nullopt;  // definitely not here: no disk read

    const std::size_t block = blockFor(key);
    if (block >= index_.size()) return std::nullopt;

    std::vector<Iterator::Item> items;
    readBlock(block, items);
    auto it = std::lower_bound(items.begin(), items.end(), key, [](const Iterator::Item& item, std::string_view k) {
        return item.key < k;
    });
    if (it == items.end() || it->key != key) return std::nullopt;  // the filter's false positive
    return LookupResult{it->tombstone, it->value};
}

// ---------------------------------------------------------------------------
// SstReader::Iterator
// ---------------------------------------------------------------------------

void SstReader::Iterator::loadBlock(std::size_t block) {
    block_ = block;
    table_->readBlock(block, items_);
    pos_ = 0;
    valid_ = true;
}

void SstReader::Iterator::seekToFirst() { loadBlock(0); }

void SstReader::Iterator::seek(std::string_view target) {
    const std::size_t block = table_->blockFor(target);
    if (block >= table_->index_.size()) {
        valid_ = false;  // past the last key
        return;
    }
    loadBlock(block);
    auto it = std::lower_bound(items_.begin(), items_.end(), target,
                               [](const Item& item, std::string_view k) { return item.key < k; });
    pos_ = static_cast<std::size_t>(it - items_.begin());  // exists: the block's last key is >= target
}

void SstReader::Iterator::next() {
    if (++pos_ < items_.size()) return;
    if (block_ + 1 < table_->index_.size()) loadBlock(block_ + 1);
    else valid_ = false;
}

}  // namespace kv::lsm
