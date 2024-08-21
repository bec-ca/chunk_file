#pragma once

#include "bee/bytes.hpp"
#include "bee/data_buffer.hpp"
#include "bee/file_path.hpp"
#include "bee/or_error.hpp"

namespace chunk_file {

struct ChunkFileReader {
 public:
  using ptr = std::unique_ptr<ChunkFileReader>;

  static bee::OrError<ptr> open(const bee::FilePath& file_path);

  virtual ~ChunkFileReader();

  [[nodiscard]] virtual bee::OrError<std::optional<bee::Bytes>> read_next() = 0;

 private:
};

struct ChunkFileWriter {
 public:
  using ptr = std::unique_ptr<ChunkFileWriter>;

  virtual ~ChunkFileWriter();

  static bee::OrError<ptr> create(const bee::FilePath& file_path);

  [[nodiscard]] virtual bee::OrError<> write(const bee::Bytes& content) = 0;

 private:
};

} // namespace chunk_file
