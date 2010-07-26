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
//
// @author Ying-Yi Liang

#include "Common.h"
#include "ScribeServer.h"

using namespace facebook::fb303;
using namespace scribe;
using namespace scribe::thrift;
using namespace std;

void printUsage(const char* programName) {
  cout << "Usage: " << programName << " [-p port] [-c config_file]" << endl;
}

int main(int argc, char **argv) {

  try {
    /* Increase number of fds */
    struct rlimit fdLimits = {65535,65535};
    if (-1 == setrlimit(RLIMIT_NOFILE, &fdLimits)) {
      LOG_OPER("setrlimit error (setting max fd size)");
    }

    int nextOption;
    const char* const shortOptions = "hp:c:";
    const struct option longOptions[] = {
      { "help",   0, NULL, 'h' },
      { "port",   0, NULL, 'p' },
      { "config", 0, NULL, 'c' },
      { NULL,     0, NULL, 'o' },
    };

    unsigned long port = 0;  // this can also be specified in the conf file,
                                 // which overrides the command line
    string configFile;
    while (0 < (nextOption = getopt_long(argc, argv, shortOptions,
                                         longOptions, NULL))) {
      switch (nextOption) {
      default:
      case 'h':
        printUsage(argv[0]);
        exit(0);
      case 'c':
        configFile = optarg;
        break;
      case 'p':
        port = strtoul(optarg, NULL, 0);
        break;
      }
    }

    // assume a non-option arg is a config file name
    if (optind < argc && configFile.empty()) {
      configFile = argv[optind];
    }

    // seed random number generation with something reasonably unique
    srand(time(NULL) ^ getpid());

    g_handler.reset(new ScribeHandler(port, configFile));
    g_handler->setStatus(STARTING);
    g_handler->initialize();

    startServer(); // never returns

  } catch(const std::exception& e) {
    LOG_OPER("Exception in main: %s", e.what());
  }

  LOG_OPER("scribe server exiting");
  return 0;
}
