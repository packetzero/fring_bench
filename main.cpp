#include <string>
#include <map>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sstream>
#include <unordered_set>

namespace rj = rapidjson;

typedef std::map<std::string,std::string> Row;

bool FLAGS_events_cache_format_json = true;

struct Status {
  int code;
  std::string msg;
  Status(int c, std::string m = "") : code(c), msg(m) {}
  bool ok() const { return code == 0; }
  operator bool() const { return ok(); }
  static Status success() { return Status(0); }
  static Status failure(std::string msg) { return Status(1, msg); }
  int getCode() const { return code; }
};

rj::Document doc;

uint32_t crc(const std::string &s) {
  uint32_t val = 0;
  const char *p = s.data();
  const char *end = s.data() + s.length();
  while (p < end) {
    val += (int8_t)*p++;
  }
  return val;
}

/*
 * An event cache table can have about 50,000 rows.
 * Creating new Row objects or reusing a Row object and doing a Row.clear()
 * for every row in event cache table of 50,000 rows can add some overhead.
 * If most rows contain the same set of columns, keeping the same row
 * object, but overwriting the columns with the new values can save time.
 * About 0.1 seconds per query of 50,000 rows.  This is an object that
 * can help the binary row encoder repeatedly calculate the hash of
 * columns used by the current row.  The decoder can then compare the
 * hash value with previous row, to know if the row object needs to
 * be cleared, or leave as is because all columns will be overwritten.
 */
struct StringHash {
  StringHash(uint32_t maxlen = 1024) : _workingString(), _hasher() {
    _workingString.reserve(maxlen);
    _p = (char *)_workingString.data();
    _e = (char *)_workingString.data() + _workingString.capacity();
  }

  void clear() {
    _p = (char *)_workingString.data();
    _workingString.clear();
  }

  /*
   * Appends str to the end of the current working string.
   */
  void add(const std::string &str) {
    if (remaining() < str.length()) { return; }
    strncpy(_p, str.c_str(), str.length());
    _p += str.length();
  }

  /*
   * Calculates the hash on current working string and
   * sets _hashValue.
   */
  void finalize() {
    _workingString.resize(_p - _workingString.data());
    _hashValue = _hasher(_workingString);
  }

  /*
   * @return _hashValue Caller must call finalize() if any
   * changes have been made to working string.
   */
  uint32_t hash() { return _hashValue; }

  size_t remaining() { return (size_t)(_e - _p); }

protected:
  std::string _workingString;
  char *_p, *_e;
  uint32_t _hashValue {0};
  std::hash<std::string> _hasher;
};


static inline size_t CalcSimpleStringMapEncodeSize(const std::map<std::string,std::string> &vec, StringHash &sh)
{
  size_t len = 0;
  len += 4; // keyhash (24) | num keys (8)
  sh.clear();
  for (const auto &it : vec) {
    len += sizeof(uint32_t) + it.first.size() + it.second.size();
    sh.add(it.first);
  }
  sh.finalize();
  return len;
}

/*
 * Encodes each field in string map in the following
 * byte format.
 * NOTE: This encoding puts an artificial limit on the max size of an
 * event column value of 2^24  (16MB).
 *
 *   [32-bit header][name bytes][value bytes - if any]
 * where header dword is:
 *   [ name len (8-bits) |value length (24-bits)   ]
 * @param sm Row to encode
 * @param dest  Upon return, dest will be resized and populated with encoded value.
 * @return true on error, false on success
 */
bool SimpleStringMapEncode(const std::map<std::string,std::string> &sm, std::string &dest, StringHash &sh) //uint32_t &keyscrc)
{
  size_t dest_size = CalcSimpleStringMapEncodeSize(sm, sh);//keyscrc);
  if (dest_size == 0) { return true; }

  dest.resize(dest_size);

  char *p = (char *)dest.c_str();

  // write keyhash | num keys
  *((uint32_t*)p) = (uint32_t)(sh.hash() << 8 | (uint8_t)sm.size());
  p += sizeof(uint32_t);

  for (const auto &it : sm) {
    auto namelen = it.first.size();
    auto valuelen = it.second.size();
    if (namelen >= 0x00FF) {
      //LOG(WARNING) << "Row name too long:" << it.first;
      return true;
    }
    // write header
    *((uint32_t*)p) = (uint32_t)(namelen << 24 | valuelen);
    p += 4;
    strncpy(p, it.first.c_str(), namelen);
    p += namelen;
    if (valuelen > 0) {
      strncpy(p, it.second.c_str(), valuelen);
      p += valuelen;
    }
  }
  return false;
}

/**
 * Decodes the output of SimpleStringMapEncode() and populates sm.
 * @param sm Should have empty()==true on enter.
 * @param encoded bytes string.
 * @return true on error, false on success.
 */
bool SimpleStringMapDecode(std::map<std::string,std::string> &sm, const std::string &encoded, uint32_t &lastKeyHash)
{
  if (encoded.size() == 0) { return true; }

  char *p = (char *)encoded.c_str();
  char *end = p + encoded.size();

  // fields are prepended by a hash value of keys

  uint32_t keyHashValue = *((uint32_t*)p);
  p += 4;

  // optimization : if keys are the same in this row as previous, no need to clear

  if (lastKeyHash != keyHashValue) {
    sm.clear();
  }
  lastKeyHash = keyHashValue;

  while (p < end) {
    uint32_t hdr = *((uint32_t*)p);
    p += 4;
    int name_len = hdr >> 24;
    int value_len = hdr & 0x00FFFFFF;
    if (name_len <= 0 || value_len < 0 || (p + name_len + value_len) > end ) {
      return true;
    }
    char *v = p + name_len;
    sm[std::string(p,p+name_len)]=std::string(v,v+value_len);
    p += name_len + value_len;
  }
  return false;
}


  inline rj::Value SVAL(const std::string &str) {
    rj::Value retval;
    retval.SetString(str.c_str(), str.size(), doc.GetAllocator());
    return retval;
  }

  void _serializeRow(rj::Value &arr, const Row &row) {
    rj::Value obj(rj::kObjectType);

    for (auto &it : row) {
      obj.AddMember(SVAL(it.first),SVAL(it.second),doc.GetAllocator());
    }
    arr.PushBack(rj::Value(obj, doc.GetAllocator()).Move(),
                 doc.GetAllocator());
  }

  /**
   * Serializes the current data snapshot into dest.
   */
   Status  serializeRowJSON(const Row &row, std::string &dest)  {
    doc.SetArray();
    doc.Clear();
    //for (auto &row : _results) {
      _serializeRow(doc, row);
    //}

    // render to string

    rj::StringBuffer buffer;
    rj::Writer<rj::StringBuffer> writer(buffer);
    doc.Accept(writer);

    dest = buffer.GetString();
     return Status(0);
  }

  bool deserializeRow(const rj::Value& doc, Row& r) {
    if (!doc.IsObject()) {
      return true;
    }

    for (const auto& i : doc.GetObject()) {
      std::string name(i.name.GetString());
      if (!name.empty() && i.value.IsString()) {
        r[name] = i.value.GetString();
      }
    }
    return false;
  }

  Status deserializeRowJSON(Row &r, const std::string &json) {
    rj::Document doc;

    if (doc.Parse(json.c_str()).HasParseError()) {
      // TODO: log
      return Status(1);
    }
    if (!doc.IsArray()) {
      return Status(1);
    }
    for (const auto& i : doc.GetArray()) {
      //Row r;
      auto status = deserializeRow(i, r);
      if (status) {
        return status;
      }
      //_prevRows.insert(std::move(r));
      break;
    }
    return Status(0);

  }

// decode wrapper to allow for switching between JSON and SIMPLE format.
static inline Status _decode(Row &r, const std::string &value, uint32_t &lastKeyHash)
{
  if (FLAGS_events_cache_format_json) {
    r.clear();
    return deserializeRowJSON(r, value);
  }
  if (SimpleStringMapDecode(r,value, lastKeyHash)) {
    return Status(1,"failed to deserialize simple event cache row.");
  }
  return Status(0);
}

// encode wrapper to allow for switching between JSON and SIMPLE format.
static inline Status _encode(const Row &row, std::string &serialized_row, StringHash &sh)
{
  if (FLAGS_events_cache_format_json) {
    CalcSimpleStringMapEncodeSize(row, sh); //keyscrc);// calc keys crc, ignore returned size for json
    auto status = serializeRowJSON(row, serialized_row);
    
    if (serialized_row.size() > 0 && serialized_row.back() == '\n') {
      serialized_row.pop_back();
    }
    return status;
  }
  if (SimpleStringMapEncode(row, serialized_row, sh)) { //keyscrc)) {
    return Status(1,"failed to encode event cache row");
  }
  return Status(0);
}
std::vector<Row> rows = {
  {{"path","/some/actual/path/goes/here"}
    ,{"name","launchd"}
    ,{"state","running"}
    ,{"cmdline","/some/actual/path/goes/here lakdflajflkadjf ladfkjalfkja flkajdf lkfj laskdfj slfkj slfkja flksj flksj lkajf lakjf laskdjf slkfj lskdfj sldkf jsldfj salkj lfk jaflksj flksj dflskj flkj flakfj laf j"}
    ,{"cwd","/var/local/blah"}
    ,{"root","false"}

    ,{"pid","234"}
    ,{"uid","2"}
    ,{"gid","500"}
    ,{"euid","2"}
    ,{"egid","500"}
    ,{"suid","2"}
    ,{"sgid","500"}

    ,{"wired_size","25000000"}
    ,{"resident_size","55000000"}
    ,{"total_size","55000000"}
    ,{"parent","2"}
    ,{"handle_count","5"}
    ,{"disk_bytes_read","1"}

    ,{"aaa","25000000"}
    ,{"bbbb","25000000"}
    ,{"cccvc","25000000"}
    ,{"ddd","25000000"}
  }
  ,  {{"path","/some/actual/path/goes/here"}
    ,{"name","launchd"}
    ,{"state","running"}
    ,{"cmdline","/some/actual/path/goes/here lakdflajflkadjf ladfkjalfkja flkajdf lkfj laskdfj slfkj slfkja flksj flksj lkajf lakjf laskdjf slkfj lskdfj sldkf jsldfj salkj lfk jaflksj flksj dflskj flkj flakfj laf j"}
    ,{"cwd","/var/local/blah"}
    ,{"root","false"}
    
    ,{"pid","234"}
    ,{"uid","2"}
    ,{"gid","500"}
    
    ,{"wired_size","25000000"}
    ,{"resident_size","55000000"}
    ,{"total_size","55000000"}
    ,{"parent","2"}
    ,{"handle_count","5"}
    ,{"disk_bytes_read","1"}
    
    ,{"aaa","25000000"}
    ,{"bbbb","25000000"}
    ,{"cccvc","25000000"}
    ,{"ddd","25000000"}
  }

};

void test_stringmap(int totalRows, int iterations, bool decodeOnly, bool identRows)
{
  std::string s;
  uint32_t lastKeyHash = 0;
  StringHash keyHasher(1024);
  size_t num_defined_rows = rows.size();

  // same row values
  if (identRows)  { num_defined_rows = 1; }

  if (decodeOnly) {
    _encode(rows[0], s, keyHasher);
    printf("%s encoded size:%lu\n", (FLAGS_events_cache_format_json ? "JSON":"BIN"), s.size());
  }

  size_t n =0;
  
  for (int i=0; i < iterations; i++) {
    Row tmprow;
    for (int j=0;j < totalRows; j++) {
      if (!decodeOnly) {
        _encode(rows[i % num_defined_rows], s, keyHasher);//keyscrc);
      }
      _decode(tmprow, s, lastKeyHash);
      n += tmprow.size();
    }
  }
}

// results:
// 71% faster overall
// 11.35s json,rows=50000, iter=20,decodeOnly=false
//  1.98s bin,rows=50000, iter=20,decodeOnly=false

// 80% faster on decode
//  6.94s json,rows=50000, iter=20,decodeOnly=true
//  1.4s bin,rows=50000, iter=20,decodeOnly=true

// JSON encoded size: 606
// BIN  encoded size: 561

int main(int argc, char *argv[])
{
  FLAGS_events_cache_format_json = false;
  bool decodeOnly = true;
  bool identRows = true;
  test_stringmap(50000, 1, decodeOnly, identRows);
  return 0;
}
