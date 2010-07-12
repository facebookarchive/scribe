/* Functions for latency computation
 */

#ifndef SCRIBE_TIME_LATENCY_H
#define SCRIBE_TIME_LATENCY_H

#include "Common.h"
#include <sys/time.h>
#include <ctime>

namespace scribe {

const string kMetadataTimestamp = "timestamp";

// current time in milliseconds
unsigned long getCurrentTimeStamp();

// check if metadata is present and if so
// are the timestamp field present
bool isTimeStampPresent(const LogEntry& entry);

// get timestamp in ms
unsigned long getTimeStamp(const LogEntry& entry);

// update timestamp
void updateTimeStamp(LogEntry& entry, unsigned long ts);

// remove timestamp from the message
void removeTimeStamp(LogEntry& entry);


// current time in milliseconds
inline unsigned long getCurrentTimeStamp() {
  return scribe::clock::nowInMsec();
}

// make it an inline function to improve efficiency
inline bool isTimeStampPresent(const LogEntry& message) {
  return (
    message.__isset.metadata &&
    message.metadata.find(kMetadataTimestamp) != message.metadata.end()
  );
}

} //! namespace scribe

#endif //! SCRIBE_TIME_LATENCY_H
