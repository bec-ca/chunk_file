#include "chunk_file.hpp"

#include "bee/binary_format.hpp"
#include "bee/file_path.hpp"
#include "bee/file_reader.hpp"
#include "bee/file_writer.hpp"

namespace chunk_file {

////////////////////////////////////////////////////////////////////////////////
// ChunkFileReader
//

namespace {

class ChunkFileReaderImpl final : public ChunkFileReader {
 public:
  ChunkFileReaderImpl(bee::FileReader::ptr&& reader)
      : _reader(std::move(reader))
  {}

  virtual ~ChunkFileReaderImpl() {}

  static bee::OrError<ptr> open(const bee::FilePath& file_path)
  {
    bail(reader, bee::FileReader::open(file_path));
    return std::make_unique<ChunkFileReaderImpl>(std::move(reader));
  }

  virtual bee::OrError<std::optional<bee::Bytes>> read_next() override
  {
    bail(is_eof, _maybe_read_more(16));
    if (is_eof) { return std::nullopt; }
    bail(chunk_size, bee::BinaryFormat::read_var_uint(_buf));
    bail_unit(_maybe_read_more(chunk_size + 16));
    auto chunk = _buf.read_bytes(chunk_size);
    if (chunk.size() < chunk_size) { return bee::Error("Chunk data missing"); }
    return chunk;
  }

 private:
  bee::OrError<bool> _maybe_read_more [[nodiscard]] (size_t min_bytes)
  {
    if (_buf.size() >= min_bytes) { return false; }
    auto to_read = std::max<size_t>(min_bytes, 1024);
    bee::Bytes bytes;
    bail(bytes_read, _reader->read(bytes, to_read));
    if (bytes_read == 0 && _buf.size() == 0) { return true; }
    _buf.write(std::move(bytes));
    return false;
  }

  bee::FileReader::ptr _reader;

  bee::DataBuffer _buf;
};

} // namespace

ChunkFileReader::~ChunkFileReader() {}

bee::OrError<ChunkFileReader::ptr> ChunkFileReader::open(
  const bee::FilePath& file_path)
{
  return ChunkFileReaderImpl::open(file_path);
}

////////////////////////////////////////////////////////////////////////////////
// ChunkFileWriter
//

namespace {

class ChunkFileWriterImpl final : public ChunkFileWriter {
 public:
  ChunkFileWriterImpl(bee::FileWriter::ptr&& writer)
      : _writer(std::move(writer))
  {}

  virtual ~ChunkFileWriterImpl() {}

  static bee::OrError<ptr> create(const bee::FilePath& file_path)
  {
    bail(writer, bee::FileWriter::create(file_path));
    return std::make_unique<ChunkFileWriterImpl>(std::move(writer));
  }

  virtual bee::OrError<> write(const bee::Bytes& content) override
  {
    bee::DataBuffer buf;
    bee::BinaryFormat::write_var_uint(buf, content.size());
    buf.write(content);
    bail_unit(_writer->write(buf));
    return bee::ok();
  }

 private:
  bee::FileWriter::ptr _writer;
};

} // namespace
  //
ChunkFileWriter::~ChunkFileWriter() {}

bee::OrError<ChunkFileWriter::ptr> ChunkFileWriter::create(
  const bee::FilePath& file_path)
{
  return ChunkFileWriterImpl::create(file_path);
}

} // namespace chunk_file
