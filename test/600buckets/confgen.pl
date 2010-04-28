#!/usr/bin/perl -w

use strict;

use Getopt::Long;

my $category = "ad_imps_2";
my $midtierhost = '127.0.0.1';
my $midtierport = 9999;
my $receiverhost = '127.0.0.1';
my $receiverporteven = 9998;
my $receiverportodd = 9999;
my $numbuckets = 600;
my $help = '';
my $outputdir = ".";
my $scribefilepath ="/tmp/scribetest/600buckets";

my $result = GetOptions (
      "category=s" => \$category,
      "midtierhost=s" => \$midtierhost,
      "midtierport=i" => \$midtierport,
      "receiverhost=s" => \$receiverhost,
      "receiverporteven=i" => \$receiverporteven,
      "receiverportodd=i" => \$receiverportodd,
      "numbuckets=i" => \$numbuckets,
      "outputdir=s" => \$outputdir,
      "scribefilepath=s" => \$scribefilepath,
      "help" => \$help);

if ($help) {
  print <<EOD;
perl $0   --midtierhost mhost --midtierport mport --receiverhost host 
          --receiverporteven port1 --receiverportodd port2 
          --numbuckets numbuckets --outputdir --help
          --category category --scribefilepath scribefilepath
Usage:
help: print this message
miditerhost: host name of the midtier, default to 127.0.0.1
midtierport: port number of the midtier, default to 9999
receiverhost: the host name for the receiver, default to 127.0.0.1
receiverporteven: reciver that will accept messages with even bucket id, 
                  default to 9998.
receiverportodd: reciver that will accept messages with odd bucket id, 
                  default to 9999.
numbickets: number of buckets, default to 600
outputdir: direcotry to output the conf files, default to ".".
category: scribe category that will be used in the test, default to ad_imps_2
scribefilepath: the directory underwhich scribe file store will log message to.


EOD

  exit 0;
}

#opening files
open MIDTIERCONF, ">$outputdir/scribe.midtier.conf" or 
                  die "Can't open $outputdir/scribe.midtier.conf: $!";
open RECEIVERODDCONF, ">$outputdir/scribe.sinker.file.odd.conf" or 
                  die "Can't open $outputdir/scribe.sinker.file.odd.conf: $!";
open RECEIVEREVENCONF, ">$outputdir/scribe.sinker.file.even.conf" or 
                  die "Can't open $outputdir/scribe.sinker.file.even.conf: $!";
open RECEIVERCONF, ">$outputdir/scribe.sinker.conf" or 
                  die "Can't open $outputdir/scribe.sinker.conf: $!";
open SENDERCONF, ">$outputdir/scribe.sender.conf" or 
                  die "Can't open $outputdir/scribe.sender.conf: $!";


# Generating scribe.midtier.conf
my $midtierheader = <<EOD;
port=$midtierport
check_interval=1
new_thread_per_category=no
num_thrift_server_threads=3

# max queue size is a global configure var
# it need to be the largest of all stores
# target_write_size: i.e. 50000000
# it is used by the scribed to throttle 
# requests, i.e. it is the max Bps
# a scribe server will allow for a top
# level store. it should be larget than
# the largest target_write_size
max_queue_size=75000000
# we also need to tweak max_msg_per_second
# for 10MB/s of a lower bound of 200 messages
# per second, we have 10MB/s / 200B = 50k
# so it has to be at least 50k
max_msg_per_second=2000000
<store>
category=$category
type=bucket
bucket_type=key_modulo
bucket_range=$numbuckets
num_buckets=$numbuckets
delimiter=1
# make write every 6 seconds
max_write_interval=6
# for 10MBps input rate,  max_write_interval of 6 sec,
# 10MB/s * 6s = 60MB
target_write_size=60000000

  <bucket0>
    type=buffer
    retry_interval=30
    retry_interval_range=2
    buffer_send_rate=1
    #streaming while flushing buffer
    flush_streaming=yes

    <primary>
      type=network
      remote_host=$receiverhost
      remote_port=$receiverporteven
      use_conn_pool=yes
      timeout=2000
    </primary>
    <secondary>
      type=file
      fs_type=std
      file_path=$scribefilepath/midtier/bid0
      base_filename=$category
      max_size=5000000
    </secondary>
  </bucket0>

EOD

my $midtierbody = <<EOD;
  <bucket%d>
    type=buffer
    retry_interval=30
    retry_interval_range=2
    #streaming while flushing buffer
    flush_streaming=yes
    # since we have 600 buckets, and 5M of 
    # buffer file each. we can't afford to
    # send 600 x 2 x 5M = 6G of data out
    # in buffer sending phase. Tune down the
    # buffer_send_rate down to 1 first.
    buffer_send_rate=1
    # buffer_bypass_max_ratio multiple
    # max_queue_size is the max size that 
    # during buffer flushing phase a buffer
    # store should bypass enqueued messages. 
    buffer_bypass_max_ratio=0.75

    <primary>
      type=network
      remote_host=%s
      remote_port=%d
      use_conn_pool=yes
      timeout=1500
    </primary>
    <secondary>
      type=file
      fs_type=std
      file_path=$scribefilepath/midtier/bid%d
      base_filename=$category
      max_size=5000000
    </secondary>
  </bucket%d>
EOD

print MIDTIERCONF $midtierheader;

for (my $i = 1; $i <= $numbuckets; ++$i) {
  print MIDTIERCONF "  #Bucket $i\n";
  my $remoteport = ($i % 2 == 0 ? $receiverporteven : $receiverportodd); 
  print MIDTIERCONF sprintf($midtierbody, $i, $receiverhost, $remoteport, $i, $i) . "\n";
}

print MIDTIERCONF <<EOD;
</store>

EOD


# Generate scribe.sinker.conf
print RECEIVERCONF <<EOD;
max_msg_per_second=100000000
max_queue_size=500000000
check_interval=1
num_thrift_server_threads=3

#ad_imps_2 sinker
<store>
category=$category
type=null
</store>

# DEFAULT - This shouldn't actually get used on this machine; we should only
# be receiving the categories configured above. Have a default category just
# in case, but make sure it will not write to the same location as the other
# default categories.
<store>
category=default
type=file
max_write_interval=5

fs_type=std
file_path=$scribefilepath/receiver/default
base_filename=thisisoverwritten
max_size=30000000
</store>

EOD

#Generate scribe.sinker.file.even.conf
print  RECEIVEREVENCONF <<EOD;
port=$receiverporteven
check_interval=1
new_thread_per_category=no
num_thrift_server_threads=3
max_queue_size=100000000
max_msg_per_second=100000000
<store>
category=$category
type=bucket
bucket_type=key_modulo
bucket_range=$numbuckets
num_buckets=$numbuckets
delimiter=1
max_write_interval=5
target_write_size=5000000

EOD

my $body = <<EOD;
  <bucket%d>
    type=file
    fs_type=std
    file_path=$scribefilepath/sinker/odd/bid%d
    add_newlines=1
    base_filename=$category
  </bucket%d>
EOD

for (my $i = 0; $i <= $numbuckets; $i++) {
  print RECEIVEREVENCONF sprintf($body, $i, $i, $i) . "\n";
}

print RECEIVEREVENCONF "</store>\n";


#Generate scribe.sinker.file.even.conf
print  RECEIVERODDCONF <<EOD;
port=$receiverportodd
check_interval=1
new_thread_per_category=no
num_thrift_server_threads=3
max_queue_size=100000000
max_msg_per_second=100000000
<store>
category=$category
type=bucket
bucket_type=key_modulo
bucket_range=$numbuckets
num_buckets=$numbuckets
delimiter=1
max_write_interval=5
target_write_size=5000000

EOD

$body = <<EOD;
  <bucket%d>
    type=file
    fs_type=std
    file_path=$scribefilepath/sinker/even/bid%d
    add_newlines=1
    base_filename=$category
  </bucket%d>
EOD

for (my $i = 0; $i <= $numbuckets; $i++) {
  print RECEIVERODDCONF sprintf($body, $i, $i, $i) . "\n";
}

print RECEIVERODDCONF "</store>\n";

#Generate scribe.sender.conf
print SENDERCONF <<EOD;
max_queue_size=100000000
max_msg_per_second=1000000
check_interval=1
num_thrift_server_threads=3

# AD_IMPS_2
<store>
category=$category
type=network
remote_host=$midtierhost
remote_port=$midtierport
use_conn_pool=yes
timeout=1500
target_write_size=800000
</store>

#<store>
#category=$category
#type=buffer
#flush_streaming=yes
#retry_interval=3
#retry_interval_range=1
#  <primary>
#    type=network
#    remote_host=$midtierhost
#    remote_port=$midtierport
#    use_conn_pool=yes
#    timeout=1500
#  </primary>
#  <secondary>
#    type=file
#    fs_type=std
#    max_write_interval=5
#    file_path=$scribefilepath/sender/$category
#    base_filename=$category
#    max_size=5000000
#  </secondary>
#</store>

# DEFAULT - This shouldn't actually get used on this machine; we should only
# be receiving the categories configured above. Have a default category just
# in case, but make sure it will not write to the same location as the other
# default categories.
<store>
category=default
type=file
max_write_interval=5
fs_type=std
file_path=$scribefilepath/sender/default
base_filename=thisisoverwritten
max_size=30000000
</store>

EOD

