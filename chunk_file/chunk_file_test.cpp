#include "chunk_file.hpp"

#include "bee/testing.hpp"

namespace chunk_file {
namespace {

const bee::FilePath filename("/tmp/blackbit_chunk_file_test.cf");

TEST(basic_read_and_write)
{
  {
    must(writer, ChunkFileWriter::create(filename));
    must_unit(writer->write(bee::Bytes("hello")));
    must_unit(writer->write(bee::Bytes("world")));
  }
  {
    must(reader, ChunkFileReader::open(filename));
    while (true) {
      must(chunk_opt, reader->read_next());
      if (!chunk_opt.has_value()) { break; }
      P(*chunk_opt);
    }
  }
}

TEST(read_and_write_many)
{
  {
    must(writer, ChunkFileWriter::create(filename));
    for (int i = 0; i < 10000; i++) {
      must_unit(writer->write(bee::Bytes(F("hello: $", i))));
    }
  }
  {
    must(reader, ChunkFileReader::open(filename));
    int count = 0;
    while (true) {
      must(chunk_opt, reader->read_next());
      if (!chunk_opt.has_value()) { break; }
      bee::Bytes expected(F("hello: $", count));
      if (*chunk_opt == expected) {
        count++;
      } else {
        P("Got unexpected chunk: $", *chunk_opt);
      }
    }
    P(count);
  }
}

TEST(read_and_write_large_chunks)
{
  std::vector<bee::Bytes> chunks;
  for (int i = 0; i < 10; i++) {
    std::string chunk;
    for (int j = 0; j < 10000; j++) { chunk += F("hello: $", i * j + i); }
    chunks.push_back(bee::Bytes(chunk));
  }
  {
    must(writer, ChunkFileWriter::create(filename));
    for (const auto& chunk : chunks) { must_unit(writer->write(chunk)); }
  }
  {
    must(reader, ChunkFileReader::open(filename));
    int chunk_id = 0;
    while (true) {
      must(chunk_opt, reader->read_next());
      if (!chunk_opt.has_value()) { break; }
      if (*chunk_opt != chunks[chunk_id]) {
        P("Got unexpected chunk: $", *chunk_opt);
      }
      chunk_id++;
    }
    P(chunk_id);
  }
}

} // namespace
} // namespace chunk_file
