echo '         Time           Device     read(KB)     write(KB)'
iostat -d -x sda -k 1 | perl -pe '@a = split(/\s+/); $_ = localtime() . "\t$a[0]\t$a[7]\t$a[8]\n"; $| = 1;' | grep sda
