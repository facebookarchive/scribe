#!/bin/sh

##  Copyright (c) 2011 Qualtrics Labs
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
## See accompanying file LICENSE or visit the Scribe site at:
## http://developers.facebook.com/scribe/

#
# This generates a pair of self-signed SSL certificates to distribute and use with Scribe over SSL.
# See exampleSSLcentral.conf for more details
#

set -e
mkdir -p cert
cd cert

# Make up a random passphrases
head -c 81 /dev/urandom | base64 -w0 > passserver
head -c 81 /dev/urandom | base64 -w0 > passclient

# Generate the keys
openssl genrsa -des3 -passout file:passserver -out server.key 4096
openssl genrsa -des3 -passout file:passclient -out client.key 4096

# Remove the passphrase
openssl rsa -in server.key -passin file:passserver -out server.key
openssl rsa -in client.key -passin file:passclient -out client.key

# Generate a CSR
openssl req -new -key server.key -out server.csr
openssl req -new -key client.key -out client.csr

# Sign the CSR ourselves:
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt
openssl x509 -req -days 365 -in client.csr -signkey client.key -out client.crt

# Get things organized:
rm passserver passclient server.csr client.csr

echo "Pair created in cert/*"