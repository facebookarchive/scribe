/* Functions for latency computation
 */

#ifndef TIME_LATENCY_H
#define TIME_LATENCY_H

#include "common.h"
#include <sys/time.h>
#include <ctime>

using std::string;
using scribe::thrift::LogEntry;

extern const string METADATA_TIMESTAMP;
extern const string METADATA_HOPCOUNT;

// current time in milliseconds
unsigned long getCurrentTimeStamp();

// check if metadata is present and if so
// are the timestamp field present
bool isTimeStampPresent(const LogEntry&);

// get timestamp in ms
unsigned long getTimeStamp(const LogEntry&);

// update timestamp
void updateTimeStamp(LogEntry&, unsigned long ts);

// remove timestamp from the message
void removeTimeStamp(LogEntry&);


// current time in milliseconds
inline unsigned long getCurrentTimeStamp() {
  return scribe::clock::nowInMsec();
}

// make it an inline function to improve efficiency
inline bool isTimeStampPresent(const LogEntry& message) {
  return (
    message.__isset.metadata &&
    message.metadata.find(METADATA_TIMESTAMP) != message.metadata.end()
  );
}

#endif
