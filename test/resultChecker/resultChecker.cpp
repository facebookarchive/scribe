//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/


#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#define MAX_MESSAGE_LENGTH 1024

void usage() {
  fprintf(stderr, "usage: resultChecker clientname file(s)\n");
  fprintf(stderr, "Reads files and counts log entries for the specified client.\n");
  fprintf(stderr, "Prints the number of messages and out of order messages in each file.\n");
  fprintf(stderr, "Entries must be formatted (\"%%s-%%d...\", client_name, sequence_number)\n");
}

int main(int argc, char** argv) {

  if (argc < 3) {
    usage();
    return 1;
  }

  std::string clientname(argv[1]);

  int last_entry = -1;
  int bad_this_file = 0;
  int entries_this_file = 0;
  int total_bad = 0;
  int total_entries = 0;

  for (int i = 2; i < argc; ++i) {

    entries_this_file = 0;
    bad_this_file = 0;

    std::ifstream infile(argv[i]);
    if (!infile.good()) {
      fprintf(stderr, "Failed to open input file: %s\n", argv[i]);
      continue;
    }
  
    while (infile.good()) {
      char buffer[MAX_MESSAGE_LENGTH];
      buffer[0] = 0;
      while (0 == infile.peek()) {
        infile.get();
      }
      infile.getline(buffer, MAX_MESSAGE_LENGTH - 1);
      if (buffer[0] != 0) {
        char* separator = strchr(buffer, '-');
        if (separator && buffer + strlen(buffer) > separator) {
          char name[MAX_MESSAGE_LENGTH];
          strncpy(name, buffer, separator - buffer);
          name[separator - buffer] = 0;
          int entry = atoi(separator + 1);
          if (0 == clientname.compare(name)) {
            ++entries_this_file;
            if (entry != last_entry + 1) {
              fprintf(stderr, "Out of order entry: <%d> follows <%d>\n", entry, last_entry);
              ++bad_this_file;
            }
            last_entry = entry;
          } // client matches
        } // line was parsed
      } // line isn't empty
    } // for each line
    infile.close();
   
    printf("File <%s>: <%d> total <%d> out of order\n", argv[i], entries_this_file, bad_this_file);
    total_bad += bad_this_file;
    total_entries += entries_this_file;

  } // for each requested file

  printf("Total: <%d> matching entries <%d> out of order\n", total_entries, total_bad);

  return 0;
}
